/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.cutlass.qwp;

import io.questdb.Bootstrap;
import io.questdb.FactoryProviderImpl;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropServerConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerConfiguration;
import io.questdb.ServerMain;
import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpCookieHandler;
import io.questdb.cutlass.http.HttpCookieHandlerImpl;
import io.questdb.cutlass.http.HttpSessionStore;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cutlass.http.HttpConstants.SESSION_COOKIE_NAME;
import static io.questdb.test.cutlass.http.HttpUtils.assertChunkedBody;
import static io.questdb.test.cutlass.http.HttpUtils.assertSessionCookie;
import static io.questdb.test.cutlass.http.HttpUtils.awaitStatusCode;
import static io.questdb.test.cutlass.http.HttpUtils.newHttpRequest;

public class QwpBrowserSessionAuthTest extends AbstractBootstrapTest {
    private static final String PASSWORD = "quest";
    private static final String USER = "admin";

    @Before
    @Override
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration(
                PropertyKey.HTTP_USER.getPropertyPath() + "=" + USER,
                PropertyKey.HTTP_PASSWORD.getPropertyPath() + "=" + PASSWORD
        ));
        dbPath.parent().$();
    }

    @Test
    public void testMissingSessionIsRejectedBeforeUpgrade() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain questdb = new ServerMain(getServerMainArgs())) {
                questdb.start();
                Assert.assertTrue(webSocketUpgrade("/write/v4", null).startsWith("HTTP/1.1 401"));
                Assert.assertTrue(webSocketUpgrade("/read/v1", null).startsWith("HTTP/1.1 401"));
            }
        });
    }

    @Test
    public void testSessionCookieAuthenticatesIngressAndEgress() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain questdb = new ServerMain(getServerMainArgs())) {
                questdb.start();
                String sessionId = createSession();

                Assert.assertTrue(webSocketUpgrade("/write/v4", sessionId)
                        .startsWith("HTTP/1.1 101 Switching Protocols\r\n"));
                Assert.assertTrue(webSocketUpgrade("/read/v1", sessionId)
                        .startsWith("HTTP/1.1 101 Switching Protocols\r\n"));
            }
        });
    }

    @Test
    public void testServiceAccountCookieHookRunsForIngressAndEgress() throws Exception {
        AtomicInteger serviceAccountCookieCalls = new AtomicInteger();
        Bootstrap bootstrap = new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public ServerConfiguration getServerConfiguration(Bootstrap bootstrap) throws Exception {
                        return new PropServerConfiguration(
                                bootstrap.getRootDirectory(),
                                bootstrap.loadProperties(),
                                getEnv(),
                                bootstrap.getLog(),
                                bootstrap.getBuildInformation(),
                                FilesFacadeImpl.INSTANCE,
                                bootstrap.getMicrosecondClock(),
                                (configuration, engine, freeOnExit) -> new FactoryProviderImpl(configuration) {
                                    private final HttpCookieHandler cookieHandler = new HttpCookieHandlerImpl() {
                                        @Override
                                        public boolean processServiceAccountCookie(
                                                HttpConnectionContext context,
                                                SecurityContext securityContext
                                        ) {
                                            serviceAccountCookieCalls.incrementAndGet();
                                            return true;
                                        }
                                    };

                                    @Override
                                    public @NotNull HttpCookieHandler getHttpCookieHandler() {
                                        return cookieHandler;
                                    }
                                }
                        );
                    }
                },
                getServerMainArgs()
        );

        assertMemoryLeak(() -> {
            try (ServerMain questdb = new ServerMain(bootstrap)) {
                questdb.start();
                String sessionId = createSession();
                serviceAccountCookieCalls.set(0);

                Assert.assertTrue(webSocketUpgrade("/write/v4", sessionId)
                        .startsWith("HTTP/1.1 101 Switching Protocols\r\n"));
                Assert.assertEquals(1, serviceAccountCookieCalls.get());

                Assert.assertTrue(webSocketUpgrade("/read/v1", sessionId)
                        .startsWith("HTTP/1.1 101 Switching Protocols\r\n"));
                Assert.assertEquals(2, serviceAccountCookieCalls.get());
            }
        });
    }

    @Test
    public void testUpgradeReturnsRotatedSessionCookie() throws Exception {
        AtomicLong currentMicros = new AtomicLong(1_760_743_438_000_000L);
        MicrosecondClock testClock = currentMicros::get;
        Bootstrap bootstrap = new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public MicrosecondClock getMicrosecondClock() {
                        return testClock;
                    }
                },
                getServerMainArgs()
        );

        assertMemoryLeak(() -> {
            try (ServerMain questdb = new ServerMain(bootstrap)) {
                questdb.start();
                String oldSessionId = createSession();
                HttpSessionStore sessionStore = questdb.getConfiguration().getFactoryProvider().getHttpSessionStore();
                HttpSessionStore.SessionInfo session = sessionStore.getSession(oldSessionId);
                Assert.assertNotNull(session);

                currentMicros.set(session.getRotateAt() + 1);
                String response = webSocketUpgrade("/write/v4", oldSessionId);
                String newSessionId = session.getSessionId().toString();

                Assert.assertNotEquals(oldSessionId, newSessionId);
                Assert.assertTrue(response.startsWith("HTTP/1.1 101 Switching Protocols\r\n"));
                Assert.assertTrue(response.contains(
                        "Set-Cookie: " + SESSION_COOKIE_NAME + "=" + newSessionId
                                + "; HttpOnly; Path=/; SameSite=Strict; Max-Age=2592000\r\n"
                ));
            }
        });
    }

    private static String createSession() {
        try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
            HttpClient.ResponseHeaders response = newHttpRequest(
                    httpClient,
                    HTTP_PORT,
                    "SELECT x FROM long_sequence(1)",
                    USER,
                    PASSWORD,
                    "true"
            );
            awaitStatusCode(response, "200");
            String sessionId = assertSessionCookie(response, false);
            assertChunkedBody(response, "{"
                    + "\"query\":\"SELECT x FROM long_sequence(1)\","
                    + "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}],"
                    + "\"timestamp\":-1,"
                    + "\"dataset\":[[1]],"
                    + "\"count\":1"
                    + "}");
            return sessionId;
        }
    }

    private static String webSocketUpgrade(String path, String sessionId) throws Exception {
        try (Socket socket = new Socket("127.0.0.1", HTTP_PORT)) {
            socket.setSoTimeout(5_000);
            StringBuilder request = new StringBuilder()
                    .append("GET ").append(path).append(" HTTP/1.1\r\n")
                    .append("Host: 127.0.0.1:").append(HTTP_PORT).append("\r\n")
                    .append("Origin: http://127.0.0.1:").append(HTTP_PORT).append("\r\n")
                    .append("Upgrade: websocket\r\n")
                    .append("Connection: Upgrade\r\n")
                    .append("Sec-WebSocket-Key: AQIDBAUGBwgJCgsMDQ4PEA==\r\n")
                    .append("Sec-WebSocket-Version: 13\r\n");
            if (sessionId != null) {
                request.append("Cookie: ").append(SESSION_COOKIE_NAME).append('=').append(sessionId).append("\r\n");
            }
            request.append("\r\n");

            OutputStream out = socket.getOutputStream();
            out.write(request.toString().getBytes(StandardCharsets.US_ASCII));
            out.flush();

            ByteArrayOutputStream response = new ByteArrayOutputStream();
            InputStream in = socket.getInputStream();
            int matched = 0;
            while (response.size() < 16_384 && matched < 4) {
                int value = in.read();
                if (value < 0) {
                    break;
                }
                response.write(value);
                if (value == (matched == 0 || matched == 2 ? '\r' : '\n')) {
                    matched++;
                } else {
                    matched = value == '\r' ? 1 : 0;
                }
            }
            return response.toString(StandardCharsets.US_ASCII);
        }
    }
}
