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

package io.questdb.test.cutlass.http.line;

import io.questdb.Bootstrap;
import io.questdb.FactoryProviderImpl;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropServerConfiguration;
import io.questdb.ServerConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AbstractAllowAllSecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cutlass.http.HttpAuthenticator;
import io.questdb.cutlass.http.HttpAuthenticatorFactory;
import io.questdb.cutlass.http.HttpConstants;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static io.questdb.test.cutlass.qwp.QwpWireTestFixtures.readChunkedBody;
import static io.questdb.test.cutlass.qwp.QwpWireTestFixtures.readHttpHeaders;

/**
 * ILP-over-HTTP re-authenticates every HTTP request, while the per-table write object is cached
 * for the lifetime of the keep-alive TCP connection. These tests pin down that the insert
 * authorization follows the identity of the request being served, and not the identity that
 * happened to create the cached write object.
 */
public class LineHttpSecurityContextTest extends AbstractBootstrapTest {
    private static final String PWD = "pwd";
    // may use the ILP endpoint, but may not insert
    private static final String USER_RO = "roUser";
    // may insert into any table
    private static final String USER_RW = "rwUser";

    @Before
    @Override
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testInsertIsAuthorizedPerRequestOnKeepAliveConnection() throws Exception {
        assertMemoryLeak(() -> {
            try (TestServerMain serverMain = authenticatingServer()) {
                serverMain.start();

                // a single socket: every request below is served by the same server-side connection
                try (Socket socket = new Socket("127.0.0.1", HTTP_PORT)) {
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(60_000);
                    final OutputStream out = socket.getOutputStream();
                    final InputStream in = socket.getInputStream();

                    // USER_RW creates 'tab' and, with it, the cached write object of this connection
                    final String created = exchange(out, in, writeRequest("tab value=1i 1000000000\n", USER_RW));
                    Assert.assertTrue(created, created.startsWith("HTTP/1.1 204"));

                    // USER_RO must not inherit USER_RW's insert permission
                    final String denied = exchange(out, in, writeRequest("tab value=2i 2000000000\n", USER_RO));
                    TestUtils.assertContains(denied, "HTTP/1.1 403");
                    TestUtils.assertContains(denied, "INSERT denied for " + USER_RO);

                    // the connection stays usable and USER_RW is still authorized on it
                    final String accepted = exchange(out, in, writeRequest("tab value=3i 3000000000\n", USER_RW));
                    Assert.assertTrue(accepted, accepted.startsWith("HTTP/1.1 204"));
                }

                serverMain.awaitTable("tab");
                serverMain.assertSql("select value from tab", """
                        value
                        1
                        3
                        """);
            }
        });
    }

    @Test
    public void testUnauthorizedIdentityCannotWriteToTableItNeverCreated() throws Exception {
        assertMemoryLeak(() -> {
            try (TestServerMain serverMain = authenticatingServer()) {
                serverMain.start();

                // separate connection, so the table exists before USER_RO ever connects
                try (Socket socket = new Socket("127.0.0.1", HTTP_PORT)) {
                    socket.setSoTimeout(60_000);
                    final String created = exchange(
                            socket.getOutputStream(),
                            socket.getInputStream(),
                            writeRequest("tab value=1i 1000000000\n", USER_RW)
                    );
                    Assert.assertTrue(created, created.startsWith("HTTP/1.1 204"));
                }

                try (Socket socket = new Socket("127.0.0.1", HTTP_PORT)) {
                    socket.setSoTimeout(60_000);
                    final String denied = exchange(
                            socket.getOutputStream(),
                            socket.getInputStream(),
                            writeRequest("tab value=2i 2000000000\n", USER_RO)
                    );
                    TestUtils.assertContains(denied, "HTTP/1.1 403");
                    TestUtils.assertContains(denied, "INSERT denied for " + USER_RO);
                }

                serverMain.awaitTable("tab");
                serverMain.assertSql("select value from tab", """
                        value
                        1
                        """);
            }
        });
    }

    private static String authHeader(String user) {
        return "Basic " + Base64.getEncoder().encodeToString((user + ':' + PWD).getBytes(StandardCharsets.UTF_8));
    }

    private static TestServerMain authenticatingServer() {
        return new TestServerMain(new Bootstrap(
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
                                    private final SecurityContext roContext = new TestSecurityContext(USER_RO, false);
                                    private final SecurityContext rwContext = new TestSecurityContext(USER_RW, true);

                                    @Override
                                    public @NotNull HttpAuthenticatorFactory getHttpAuthenticatorFactory() {
                                        return TestHttpAuthenticator::new;
                                    }

                                    @Override
                                    public @NotNull SecurityContextFactory getSecurityContextFactory() {
                                        return (principalContext, interfaceId) ->
                                                Chars.equals(USER_RO, principalContext.getPrincipal()) ? roContext : rwContext;
                                    }
                                }
                        );
                    }
                },
                getServerMainArgs()
        ));
    }

    /**
     * Sends one request and reads exactly one response off the same socket, so the caller can keep
     * using the connection afterwards. That holds for the two reply shapes this endpoint produces:
     * the bodiless 204 of a successful write, and the chunked body every error carries. A reply the
     * helper cannot frame fails here with the header block quoted, rather than leaving unread bytes
     * that would desynchronise the next exchange on this connection. A server-side disconnect shows
     * up as an empty header block, which is what makes "the requests shared one connection" an
     * assertion and not an assumption.
     */
    private static String exchange(OutputStream out, InputStream in, String request) throws Exception {
        out.write(request.getBytes(StandardCharsets.UTF_8));
        out.flush();

        final String headers = readHttpHeaders(in);
        Assert.assertFalse("server closed the connection before replying", headers.isEmpty());
        if (!Chars.contains(headers, "Transfer-Encoding: chunked")) {
            // the 204 carries no body, so the response ends at the header boundary
            Assert.assertTrue(
                    "reply is neither chunked nor a bodiless 204, so this connection cannot be reused: <<<" + headers + ">>>",
                    headers.startsWith("HTTP/1.1 204")
            );
            return headers;
        }
        return headers + readChunkedBody(in, headers);
    }

    private static String writeRequest(String lines, String user) {
        return "POST /write HTTP/1.1\r\n" +
                "Host: 127.0.0.1:" + HTTP_PORT + "\r\n" +
                "Connection: keep-alive\r\n" +
                "Authorization: " + authHeader(user) + "\r\n" +
                "Content-Length: " + lines.getBytes(StandardCharsets.UTF_8).length + "\r\n" +
                "\r\n" +
                lines;
    }

    private static class TestHttpAuthenticator implements HttpAuthenticator {
        private static final Utf8String RO_HEADER = new Utf8String(authHeader(USER_RO));
        private static final Utf8String RW_HEADER = new Utf8String(authHeader(USER_RW));
        private String principal;

        @Override
        public boolean authenticate(HttpRequestHeader headers) {
            final DirectUtf8Sequence header = headers.getHeader(HttpConstants.HEADER_AUTHORIZATION);
            if (header == null) {
                return false;
            }
            if (Utf8s.equals(RW_HEADER, header)) {
                principal = USER_RW;
                return true;
            }
            if (Utf8s.equals(RO_HEADER, header)) {
                principal = USER_RO;
                return true;
            }
            return false;
        }

        @Override
        public byte getAuthType() {
            return SecurityContext.AUTH_TYPE_CREDENTIALS;
        }

        @Override
        public CharSequence getPrincipal() {
            return principal;
        }
    }

    private static class TestSecurityContext extends AbstractAllowAllSecurityContext {
        private final boolean insertAllowed;

        private TestSecurityContext(CharSequence principal, boolean insertAllowed) {
            super(false, principal);
            this.insertAllowed = insertAllowed;
        }

        @Override
        public void authorizeInsert(TableToken tableToken) {
            if (!insertAllowed) {
                throw CairoException.authorization().put("INSERT denied for ").put(getPrincipal());
            }
        }

        @Override
        protected SecurityContext newPrincipalContext(CharSequence principal) {
            return new TestSecurityContext(principal, insertAllowed);
        }
    }
}
