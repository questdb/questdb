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

package io.questdb.test.cutlass.http;

import io.questdb.Bootstrap;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropertyKey;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.std.Misc;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.cutlass.qwp.QwpWireTestFixtures;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static io.questdb.cutlass.http.HttpConstants.SESSION_COOKIE_NAME;
import static io.questdb.test.cutlass.http.HttpUtils.*;
import static io.questdb.test.cutlass.qwp.QwpWireTestFixtures.statusLine;

/**
 * A web page can make the browser send requests to any QuestDB the browser
 * reaches, such as a server on localhost that runs without authentication.
 * The HTTP servers reject requests that a browser sends on behalf of another
 * site on every endpoint except static content and the QWP WebSocket
 * endpoints, which apply their own Origin policy, unless
 * http.browser.allowed.origins lists the site. Without authentication, they
 * also reject the host names public DNS can point at them, which DNS
 * rebinding needs, unless http.allowed.hosts lists them.
 */
public class HttpCrossSiteRequestTest extends AbstractBootstrapTest {
    private static final String CROSS_ORIGIN_REJECTED = "cross-origin browser request rejected: Origin does not match Host and is not listed in http.browser.allowed.origins";
    private static final String CROSS_SITE_REJECTED = "cross-site browser request rejected: Sec-Fetch-Site is neither same-origin nor none, and Origin is not listed in http.browser.allowed.origins";
    private static final String DUMMY_WEB_CONSOLE = "<html><body><p>Dummy Web Console</p></body></html>";
    private static final String HOST = "localhost:" + HTTP_PORT;
    private static final String HOST_NAME_REJECTED = "host name rejected: without authentication, the server serves only IP addresses, single-label names, special-use domains such as localhost, and the names listed in http.allowed.hosts";
    private static final String LISTED_ORIGIN = "https://app.example.com";
    private static final String MULTIPART_BOUNDARY = "----WebKitFormBoundaryOsOAD9cPKyHuxyBV";
    private static final String MULTIPART_CONTENT_TYPE = "multipart/form-data; boundary=" + MULTIPART_BOUNDARY;
    // a host name the attacker re-points at the server's address
    private static final String REBIND_HOST = "rebind.example.com:" + HTTP_PORT;
    private static final String SELECT_ONE_JSON = """
            {"query":"SELECT 1","columns":[{"name":"1","type":"INT"}],"timestamp":-1,"dataset":[[1]],"count":1}""";
    private static final String SELECT_ONE_RESPONSE = """
            HTTP/1.1 200 OK\r
            Server: questDB/1.0\r
            Date: Thu, 1 Jan 1970 00:00:00 GMT\r
            Transfer-Encoding: chunked\r
            Content-Type: application/json; charset=utf-8\r
            Keep-Alive: timeout=5, max=10000\r
            \r
            """ + chunk(SELECT_ONE_JSON) + "00\r\n\r\n";

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> {
            createDummyConfiguration();
            createDummyWebConsole();
        });
        dbPath.parent().$();
    }

    @Test
    public void testAnyHostServedWithAuthentication() throws Exception {
        // DNS rebinding gains nothing against authentication: the browser sends no
        // credential of this server to the attacker's host name
        assertMemoryLeak(() -> {
            try (TestServerMain ignore = startWithEnvVariables(
                    PropertyKey.HTTP_USER.getEnvVarName(), "admin",
                    PropertyKey.HTTP_PASSWORD.getEnvVarName(), "quest"
            )) {
                new SendAndReceiveRequestBuilder()
                        .withPort(HTTP_PORT)
                        .executeMany(executor -> {
                            executor.execute(
                                    "GET /exec?query=SELECT%201 HTTP/1.1\r\n" +
                                            "Host: " + REBIND_HOST + "\r\n" +
                                            "Authorization: Basic YWRtaW46cXVlc3Q=\r\n" +
                                            "\r\n",
                                    SELECT_ONE_RESPONSE
                            );
                            executor.execute(selectOne(REBIND_HOST), textResponse("401 Unauthorized", "Unauthorized"));
                        });
            }
        });
    }

    @Test
    public void testAuthenticatedCrossSiteRequestRejected() throws Exception {
        // A browser attaches cached Basic credentials and, to a same-site request, the
        // SameSite=Strict session cookie: credentials must not let another site in.
        assertMemoryLeak(() -> {
            try (
                    TestServerMain serverMain = startWithEnvVariables(
                            PropertyKey.HTTP_USER.getEnvVarName(), "admin",
                            PropertyKey.HTTP_PASSWORD.getEnvVarName(), "quest"
                    );
                    HttpClient client = HttpClientFactory.newPlainTextInstance()
            ) {
                serverMain.execute("CREATE TABLE victim AS (SELECT x FROM long_sequence(3))");
                final Endpoint drop = Endpoint.get("/exec?query=DROP%20TABLE%20victim");

                HttpClient.ResponseHeaders response = send(client, drop, true, null, "Sec-Fetch-Site", "cross-site");
                assertRejected(response, CROSS_SITE_REJECTED);

                response = send(client, drop, true, null, "Origin", "http://evil.example.com");
                assertRejected(response, CROSS_ORIGIN_REJECTED);

                // without credentials, the server answers as to any unauthenticated request
                response = send(client, drop, false, null, "Sec-Fetch-Site", "cross-site");
                awaitStatusCode(response, "401");
                assertChunkedBody(response, "Unauthorized\r\n");

                response = newHttpRequest(client, HTTP_PORT, "SELECT 1", "admin", "quest", "true");
                awaitStatusCode(response, "200");
                final String sessionId = assertSessionCookie(response, false);
                assertChunkedBody(response, SELECT_ONE_JSON);

                response = send(client, drop, false, sessionId, "Sec-Fetch-Site", "same-site");
                assertRejected(response, CROSS_SITE_REJECTED);
                serverMain.assertSql("SELECT count() FROM victim", "count\n3\n");

                // the session survives the rejected request and serves the same-origin one
                response = send(client, drop, false, sessionId, "Sec-Fetch-Site", "same-origin");
                awaitStatusCode(response, "200");
                assertChunkedBody(response, "{\"ddl\":\"OK\"}");
                serverMain.assertSql("SELECT count() FROM tables() WHERE table_name = 'victim'", "count\n0\n");
            }
        });
    }

    @Test
    public void testConnectionServesNextRequestAfterRejection() throws Exception {
        // The server reads the body of a rejected request to its end, so the
        // keep-alive connection is ready for the next request.
        assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables()) {
                final String multipartBody = multipartBody("1,2");
                final String crossSiteImport = "POST /imp?name=imported&forceHeader=true HTTP/1.1\r\n" +
                        "Host: " + HOST + "\r\n" +
                        "Sec-Fetch-Site: cross-site\r\n" +
                        "Content-Type: " + MULTIPART_CONTENT_TYPE + "\r\n" +
                        "Content-Length: " + multipartBody.length() + "\r\n" +
                        "\r\n" +
                        multipartBody;
                final String ilpLine = "ilp_written v=1i\n";
                final String crossOriginChunkedIlp = "POST /write HTTP/1.1\r\n" +
                        "Host: " + HOST + "\r\n" +
                        "Origin: http://evil.example.com\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "\r\n" +
                        Integer.toHexString(ilpLine.length()) + "\r\n" +
                        ilpLine + "\r\n" +
                        "0\r\n" +
                        "\r\n";
                final String crossSiteIlp = "POST /write HTTP/1.1\r\n" +
                        "Host: " + HOST + "\r\n" +
                        "Sec-Fetch-Site: same-site\r\n" +
                        "Content-Length: " + ilpLine.length() + "\r\n" +
                        "\r\n" +
                        ilpLine;
                final String selectOne = selectOne(HOST);

                new SendAndReceiveRequestBuilder()
                        .withPort(HTTP_PORT)
                        .executeMany(executor -> {
                            executor.execute(crossSiteImport, forbiddenResponse(CROSS_SITE_REJECTED));
                            executor.execute(selectOne, SELECT_ONE_RESPONSE);
                            executor.execute(crossOriginChunkedIlp, forbiddenResponse(CROSS_ORIGIN_REJECTED));
                            executor.execute(selectOne, SELECT_ONE_RESPONSE);
                            executor.execute(crossSiteIlp, forbiddenResponse(CROSS_SITE_REJECTED));
                            executor.execute(selectOne, SELECT_ONE_RESPONSE);
                        });

                serverMain.assertSql(
                        "SELECT count() FROM tables() WHERE table_name IN ('imported', 'ilp_written')",
                        "count\n0\n"
                );
            }
        });
    }

    @Test
    public void testCrossSiteRequestRejectedOnEveryEndpoint() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    TestServerMain serverMain = startWithEnvVariables();
                    HttpClient client = HttpClientFactory.newPlainTextInstance()
            ) {
                serverMain.execute("CREATE TABLE victim AS (SELECT x FROM long_sequence(3))");
                final Endpoint drop = Endpoint.get("/exec?query=DROP%20TABLE%20victim");
                final Endpoint imp = Endpoint.post("/imp?name=imported&forceHeader=true", MULTIPART_CONTENT_TYPE, multipartBody("1,2"));
                final Endpoint ilp = Endpoint.post("/write", null, "ilp_written v=1i\n");
                final Endpoint savePreferences = Endpoint.post("/settings?version=0", null, "{\"instance_name\":\"hijacked\"}");
                final Endpoint[] endpoints = {
                        drop,
                        imp,
                        ilp,
                        savePreferences,
                        Endpoint.get("/exp?query=SELECT%20*%20FROM%20victim"),
                        Endpoint.get("/chk?j=victim"),
                        Endpoint.get("/api/v1/sql/validate?query=SELECT%20*%20FROM%20victim"),
                        Endpoint.get("/settings"),
                        Endpoint.get("/warnings"),
                        Endpoint.get("/ping"),
                        // the health check is the default endpoint of the min HTTP server
                        new Endpoint(HTTP_MIN_PORT, "GET", "/status", null, null),
                };
                for (Endpoint endpoint : endpoints) {
                    for (String[] headers : crossSiteHeaders("localhost:" + endpoint.port)) {
                        final String[] browserHeaders = Arrays.copyOfRange(headers, 1, headers.length);
                        final HttpClient.ResponseHeaders response = send(client, endpoint, false, null, browserHeaders);
                        try {
                            assertRejected(response, headers[0]);
                        } catch (AssertionError e) {
                            throw new AssertionError(endpoint.url + " " + String.join(", ", browserHeaders), e);
                        }
                    }
                }

                // nothing has run
                serverMain.assertSql(
                        "SELECT table_name FROM tables() WHERE table_name IN ('victim', 'imported', 'ilp_written')",
                        "table_name\nvictim\n"
                );
                serverMain.assertSql("SELECT count() FROM victim", "count\n3\n");
                Assert.assertEquals(0, serverMain.getEngine().getSettingsStore().getVersion());

                // the same requests from the server's own origin run
                HttpClient.ResponseHeaders response = send(client, drop, false, null, "Sec-Fetch-Site", "same-origin");
                awaitStatusCode(response, "200");
                assertChunkedBody(response, "{\"ddl\":\"OK\"}");

                response = send(client, imp, false, null, "Sec-Fetch-Site", "same-origin");
                awaitStatusCode(response, "200");
                assertChunkedBodyContains(response, "imported");

                response = send(client, ilp, false, null, "Sec-Fetch-Site", "same-origin");
                awaitStatusCode(response, "204");

                response = send(client, savePreferences, false, null, "Sec-Fetch-Site", "same-origin");
                awaitStatusCode(response, "200");
                assertChunkedBody(response, "{\"status\":\"OK\"}\r\n");

                serverMain.assertSql(
                        "SELECT table_name FROM tables() WHERE table_name IN ('victim', 'imported', 'ilp_written') ORDER BY table_name",
                        "table_name\nilp_written\nimported\n"
                );
                Assert.assertEquals(1, serverMain.getEngine().getSettingsStore().getVersion());
            }
        });
    }

    @Test
    public void testCustomConfigurationRejectsRebindingAndCrossSiteRequests() throws Exception {
        // a configuration that sets neither allowlist, as an embedded server's may, serves
        // the host names public DNS cannot point at the server, and no other browser origin
        new HttpQueryTestBuilder()
                .withWorkerCount(1)
                .withTempFolder(root)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .run((_, _) -> new SendAndReceiveRequestBuilder().executeMany(executor -> {
                    executor.execute(selectOne("rebind.example.com:9001"), forbiddenResponse(HOST_NAME_REJECTED));
                    executor.execute(
                            "GET /exec?query=SELECT%201 HTTP/1.1\r\n" +
                                    "Host: localhost:9001\r\n" +
                                    "Sec-Fetch-Site: cross-site\r\n" +
                                    "Origin: " + LISTED_ORIGIN + "\r\n" +
                                    "\r\n",
                            forbiddenResponse(CROSS_SITE_REJECTED)
                    );
                    executor.execute(selectOne("localhost:9001"), SELECT_ONE_RESPONSE);
                }));
    }

    @Test
    public void testInvalidCrossSiteRequestKeepsItsRejection() throws Exception {
        // /exec takes GET only: the earlier rejection stands, with its own status and message
        assertMemoryLeak(() -> {
            try (
                    TestServerMain ignore = startWithEnvVariables();
                    HttpClient client = HttpClientFactory.newPlainTextInstance()
            ) {
                final Endpoint postToExec = Endpoint.post("/exec?query=SELECT%201", null, "x");
                HttpClient.ResponseHeaders response = send(client, postToExec, false, null, "Sec-Fetch-Site", "cross-site");
                awaitStatusCode(response, "405");
                assertChunkedBody(response, "Method POST not supported\r\n");

                response = send(client, postToExec, false, null, "Origin", "http://evil.example.com");
                awaitStatusCode(response, "405");
                assertChunkedBody(response, "Method POST not supported\r\n");

                new SendAndReceiveRequestBuilder()
                        .withPort(HTTP_PORT)
                        .execute(
                                "POST /exec?query=SELECT%201 HTTP/1.1\r\n" +
                                        "Host: " + REBIND_HOST + "\r\n" +
                                        "Content-Length: 1\r\n" +
                                        "\r\n" +
                                        "x",
                                textResponse("405 Method Not Allowed", "Method POST not supported")
                        );
            }
        });
    }

    @Test
    public void testListedBrowserOriginAccepted() throws Exception {
        // a web application on another origin, behind a proxy that answers CORS preflights
        final String[][] acceptedHeaders = {
                {"Sec-Fetch-Site", "cross-site", "Origin", LISTED_ORIGIN},
                {"Sec-Fetch-Site", "same-site", "Origin", LISTED_ORIGIN},
                {"Sec-Fetch-Site", "same-party", "Origin", LISTED_ORIGIN},
                // a browser that sends no Sec-Fetch-Site
                {"Origin", LISTED_ORIGIN},
        };
        final String[][] rejectedHeaders = {
                {CROSS_SITE_REJECTED, "Sec-Fetch-Site", "cross-site", "Origin", "https://other.example.com"},
                // a link or an image on a page of the listed origin sends no Origin
                {CROSS_SITE_REJECTED, "Sec-Fetch-Site", "cross-site"},
                // origins compare byte for byte, as browsers serialize them
                {CROSS_SITE_REJECTED, "Sec-Fetch-Site", "cross-site", "Origin", LISTED_ORIGIN + "/"},
                {CROSS_ORIGIN_REJECTED, "Origin", "https://other.example.com"},
                {CROSS_ORIGIN_REJECTED, "Origin", "http://app.example.com"},
                {CROSS_ORIGIN_REJECTED, "Origin", LISTED_ORIGIN + ":8443"},
                {CROSS_ORIGIN_REJECTED, "Origin", "https://APP.example.com"},
        };
        assertMemoryLeak(() -> {
            try (
                    TestServerMain serverMain = startWithEnvVariables(
                            PropertyKey.HTTP_BROWSER_ALLOWED_ORIGINS.getEnvVarName(), LISTED_ORIGIN
                    );
                    HttpClient client = HttpClientFactory.newPlainTextInstance()
            ) {
                final Endpoint health = new Endpoint(HTTP_MIN_PORT, "GET", "/status", null, null);
                for (int i = 0; i < acceptedHeaders.length; i++) {
                    final String[] headers = acceptedHeaders[i];
                    HttpClient.ResponseHeaders response = send(client, Endpoint.get("/exec?query=SELECT%201"), false, null, headers);
                    awaitStatusCode(response, "200");
                    assertChunkedBody(response, SELECT_ONE_JSON);

                    response = send(client, Endpoint.post("/write", null, "listed_origin v=" + i + "i\n"), false, null, headers);
                    awaitStatusCode(response, "204");

                    // the min HTTP server follows the same list
                    response = send(client, health, false, null, headers);
                    awaitStatusCode(response, "200");
                    assertChunkedBody(response, "Status: Healthy");
                }
                for (String[] headers : rejectedHeaders) {
                    final String[] browserHeaders = Arrays.copyOfRange(headers, 1, headers.length);
                    assertRejected(send(client, Endpoint.get("/exec?query=SELECT%201"), false, null, browserHeaders), headers[0]);
                    assertRejected(send(client, Endpoint.post("/write", null, "listed_origin v=100i\n"), false, null, browserHeaders), headers[0]);
                    assertRejected(send(client, health, false, null, browserHeaders), headers[0]);
                }
                serverMain.assertSql("SELECT v FROM listed_origin ORDER BY v", "v\n0\n1\n2\n3\n");
            }
        });
    }

    @Test
    public void testListedHostAccepted() throws Exception {
        assertMemoryLeak(() -> {
            try (TestServerMain ignore = startWithEnvVariables(
                    PropertyKey.HTTP_ALLOWED_HOSTS.getEnvVarName(), "rebind.example.com,.corp.example.com"
            )) {
                new SendAndReceiveRequestBuilder()
                        .withPort(HTTP_PORT)
                        .executeMany(executor -> {
                            executor.execute(selectOne(REBIND_HOST), SELECT_ONE_RESPONSE);
                            executor.execute(selectOne("corp.example.com:" + HTTP_PORT), SELECT_ONE_RESPONSE);
                            executor.execute(selectOne("questdb.corp.example.com:" + HTTP_PORT), SELECT_ONE_RESPONSE);
                            executor.execute(selectOne("rebind.example.net:" + HTTP_PORT), forbiddenResponse(HOST_NAME_REJECTED));
                        });
                // the min HTTP server follows the same list
                Assert.assertEquals(
                        "HTTP/1.1 200 OK",
                        statusLine(HTTP_MIN_PORT, "GET /status HTTP/1.1\r\nHost: rebind.example.com:" + HTTP_MIN_PORT + "\r\n\r\n")
                );
                new SendAndReceiveRequestBuilder()
                        .withPort(HTTP_MIN_PORT)
                        .execute(
                                "GET /status HTTP/1.1\r\nHost: rebind.example.net:" + HTTP_MIN_PORT + "\r\n\r\n",
                                forbiddenResponse(HOST_NAME_REJECTED)
                        );
            }
        });
    }

    @Test
    public void testQwpUpgradeKeepsItsOwnOriginPolicy() throws Exception {
        // QWP WebSocket endpoints admit the browser origins listed in
        // qwp.browser.allowed.origins, and a browser upgrade from such an
        // origin carries Sec-Fetch-Site: cross-site.
        assertMemoryLeak(() -> {
            try (TestServerMain ignore = startWithEnvVariables(
                    PropertyKey.QWP_BROWSER_ALLOWED_ORIGINS.getEnvVarName(), "http://app.example.com"
            )) {
                for (String path : new String[]{"/write/v4", "/read/v1"}) {
                    Assert.assertEquals(
                            "HTTP/1.1 101 Switching Protocols",
                            qwpUpgradeStatusLine(path, HOST, "http://app.example.com", "Sec-Fetch-Site: cross-site\r\n")
                    );
                    // an origin the list does not name gets the Origin check of QWP
                    Assert.assertEquals(
                            "HTTP/1.1 400 Bad Request",
                            qwpUpgradeStatusLine(path, HOST, "http://evil.example.com", "Sec-Fetch-Site: cross-site\r\n")
                    );
                }
            }
        });
    }

    @Test
    public void testRebindingHostRejectedWithoutAuthentication() throws Exception {
        // DNS rebinding: once the attacker re-points the host name of their page at the
        // server, the browser takes its requests for same-origin ones: a GET carries no
        // Origin, a POST and a WebSocket upgrade carry one that matches Host, and none
        // carries Sec-Fetch-Site, which browsers send only to https and localhost URLs
        assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE victim AS (SELECT x FROM long_sequence(3))");
                final String rebindOrigin = "http://" + REBIND_HOST;
                final String ilpLine = "ilp_written v=1i\n";
                new SendAndReceiveRequestBuilder()
                        .withPort(HTTP_PORT)
                        .executeMany(executor -> {
                            executor.execute(
                                    "GET /exec?query=DROP%20TABLE%20victim HTTP/1.1\r\n" +
                                            "Host: " + REBIND_HOST + "\r\n" +
                                            "\r\n",
                                    forbiddenResponse(HOST_NAME_REJECTED)
                            );
                            executor.execute(
                                    "POST /write HTTP/1.1\r\n" +
                                            "Host: " + REBIND_HOST + "\r\n" +
                                            "Origin: " + rebindOrigin + "\r\n" +
                                            "Content-Length: " + ilpLine.length() + "\r\n" +
                                            "\r\n" +
                                            ilpLine,
                                    forbiddenResponse(HOST_NAME_REJECTED)
                            );
                            // the host name decides, whatever the browser reports
                            executor.execute(
                                    "GET /exec?query=SELECT%201 HTTP/1.1\r\n" +
                                            "Host: " + REBIND_HOST + "\r\n" +
                                            "Sec-Fetch-Site: same-origin\r\n" +
                                            "\r\n",
                                    forbiddenResponse(HOST_NAME_REJECTED)
                            );
                            // static content as well
                            executor.execute(
                                    "GET /index.html HTTP/1.1\r\n" +
                                            "Host: " + REBIND_HOST + "\r\n" +
                                            "\r\n",
                                    forbiddenResponse(HOST_NAME_REJECTED)
                            );
                            // the host names public DNS cannot point at the server
                            for (String host : new String[]{
                                    HOST,
                                    "127.0.0.1:" + HTTP_PORT,
                                    "[::1]:" + HTTP_PORT,
                                    // Docker and Kubernetes service names
                                    "questdb:" + HTTP_PORT,
                                    "questdb.monitoring.svc.cluster.local:" + HTTP_PORT,
                                    "host.docker.internal:" + HTTP_PORT,
                                    // no Host at all
                                    null
                            }) {
                                executor.execute(selectOne(host), SELECT_ONE_RESPONSE);
                            }
                        });
                // QWP takes these for same-origin upgrades
                for (String path : new String[]{"/write/v4", "/read/v1"}) {
                    Assert.assertEquals("HTTP/1.1 403 Forbidden", qwpUpgradeStatusLine(path, REBIND_HOST, rebindOrigin, ""));
                }
                new SendAndReceiveRequestBuilder()
                        .withPort(HTTP_MIN_PORT)
                        .execute(
                                "GET /status HTTP/1.1\r\nHost: rebind.example.com:" + HTTP_MIN_PORT + "\r\n\r\n",
                                forbiddenResponse(HOST_NAME_REJECTED)
                        );
                serverMain.assertSql("SELECT count() FROM victim", "count\n3\n");
                serverMain.assertSql("SELECT count() FROM tables() WHERE table_name = 'ilp_written'", "count\n0\n");
            }
        });
    }

    @Test
    public void testRequestWithoutCrossSiteSignalAccepted() throws Exception {
        final String[][] acceptedHeaders = {
                // non-browser clients send neither header
                {},
                {"Sec-Fetch-Site", "same-origin"},
                // the user typed the URL, or opened a bookmark
                {"Sec-Fetch-Site", "none"},
                // behind a TLS-terminating proxy the browser sends an https Origin, which
                // the plaintext connection would not match, but Sec-Fetch-Site decides
                {"Sec-Fetch-Site", "same-origin", "Origin", "https://questdb.example.com"},
                // browsers send no Sec-Fetch-Site to a plaintext URL of a remote host
                {"Origin", "http://" + HOST},
        };
        assertMemoryLeak(() -> {
            try (
                    TestServerMain serverMain = startWithEnvVariables();
                    HttpClient client = HttpClientFactory.newPlainTextInstance()
            ) {
                for (int i = 0; i < acceptedHeaders.length; i++) {
                    final String[] headers = acceptedHeaders[i];
                    HttpClient.ResponseHeaders response = send(client, Endpoint.get("/exec?query=SELECT%201"), false, null, headers);
                    awaitStatusCode(response, "200");
                    assertChunkedBody(response, SELECT_ONE_JSON);

                    response = send(client, Endpoint.post("/write", null, "accepted v=" + i + "i\n"), false, null, headers);
                    awaitStatusCode(response, "204");

                    response = send(client, Endpoint.post("/imp?name=accepted_import&forceHeader=true", MULTIPART_CONTENT_TYPE, multipartBody(i + "," + i)), false, null, headers);
                    awaitStatusCode(response, "200");
                    assertChunkedBodyContains(response, "accepted_import");
                }
                serverMain.assertSql("SELECT v FROM accepted ORDER BY v", "v\n0\n1\n2\n3\n4\n");
                serverMain.assertSql("SELECT a FROM accepted_import ORDER BY a", "a\n0\n1\n2\n3\n4\n");
            }
        });
    }

    @Test
    public void testStaticContentServedToCrossSiteRequest() throws Exception {
        // links and embedded assets on other sites reach the web console's files
        assertMemoryLeak(() -> {
            try (
                    TestServerMain ignore = startWithDummyWebConsole();
                    HttpClient client = HttpClientFactory.newPlainTextInstance()
            ) {
                for (String[] headers : crossSiteHeaders(HOST)) {
                    final String[] browserHeaders = Arrays.copyOfRange(headers, 1, headers.length);
                    final HttpClient.ResponseHeaders response = send(client, Endpoint.get("/index.html"), false, null, browserHeaders);
                    awaitStatusCode(response, "200");
                    assertChunkedBody(response, DUMMY_WEB_CONSOLE);
                }
            }
        });
    }

    private static void assertRejected(HttpClient.ResponseHeaders response, String message) {
        awaitStatusCode(response, "403");
        assertChunkedBody(response, message + "\r\n");
    }

    private static String chunk(String content) {
        return String.format("%02x", content.length()) + "\r\n" + content + "\r\n";
    }

    // Each entry is the rejection message followed by header name/value pairs. The
    // Origin values are relative to the server at the given authority.
    private static String[][] crossSiteHeaders(String authority) {
        final String sameOrigin = "http://" + authority;
        return new String[][]{
                // without a listed Origin, Sec-Fetch-Site decides when present: only same-origin and none pass
                {CROSS_SITE_REJECTED, "Sec-Fetch-Site", "cross-site"},
                {CROSS_SITE_REJECTED, "Sec-Fetch-Site", "same-site"},
                {CROSS_SITE_REJECTED, "Sec-Fetch-Site", "Same-Origin"},
                {CROSS_SITE_REJECTED, "Sec-Fetch-Site", "navigate"},
                {CROSS_SITE_REJECTED, "Sec-Fetch-Site", ""},
                {CROSS_SITE_REJECTED, "Sec-Fetch-Site", "cross-site", "Origin", sameOrigin},
                {CROSS_SITE_REJECTED, "Sec-Fetch-Site", "same-site", "Origin", sameOrigin},
                // without Sec-Fetch-Site, Origin must be the origin of Host
                {CROSS_ORIGIN_REJECTED, "Origin", "http://evil.example.com"},
                // same host on another port: same site, but another origin
                {CROSS_ORIGIN_REJECTED, "Origin", "http://localhost:1"},
                {CROSS_ORIGIN_REJECTED, "Origin", "https://" + authority},
                // sandboxed frames, file: and data: documents
                {CROSS_ORIGIN_REJECTED, "Origin", "null"},
                {CROSS_ORIGIN_REJECTED, "Origin", sameOrigin + "/"},
        };
    }

    private static String forbiddenResponse(String message) {
        return textResponse("403 Forbidden", message);
    }

    private static String multipartBody(String row) {
        return "--" + MULTIPART_BOUNDARY + "\r\n" +
                "Content-Disposition: form-data; name=\"data\"\r\n" +
                "\r\n" +
                "a,b\r\n" +
                row + "\r\n" +
                "\r\n" +
                "--" + MULTIPART_BOUNDARY + "--";
    }

    private static String qwpUpgradeStatusLine(String path, String host, String origin, String extraHeaders) throws Exception {
        return statusLine(HTTP_PORT, QwpWireTestFixtures.browserUpgradeRequestWithOrigin(path, host, origin, extraHeaders));
    }

    // a GET of SELECT 1 with the given Host, or with none
    private static String selectOne(String host) {
        return "GET /exec?query=SELECT%201 HTTP/1.1\r\n" +
                (host != null ? "Host: " + host + "\r\n" : "") +
                "\r\n";
    }

    private static HttpClient.ResponseHeaders send(
            HttpClient client,
            Endpoint endpoint,
            boolean isAuthenticated,
            String sessionId,
            String... headers
    ) {
        final HttpClient.Request request = client.newRequest("localhost", endpoint.port);
        if ("POST".equals(endpoint.method)) {
            request.POST();
        } else {
            request.GET();
        }
        request.url(endpoint.url);
        if (isAuthenticated) {
            request.authBasic("admin", "quest");
        }
        if (sessionId != null) {
            request.setCookie(SESSION_COOKIE_NAME, sessionId);
        }
        for (int i = 0; i < headers.length; i += 2) {
            request.header(headers[i], headers[i + 1]);
        }
        if (endpoint.contentType != null) {
            request.header("Content-Type", endpoint.contentType);
        }
        if (endpoint.body != null) {
            request.withContent().putAscii(endpoint.body);
        }
        return request.send();
    }

    // starts the server without extracting the web console build, which CI tests carry on
    // the classpath and which would replace the dummy web console
    private static TestServerMain startWithDummyWebConsole() {
        final TestServerMain serverMain = new TestServerMain(new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public boolean useSite() {
                        return false;
                    }
                },
                getServerMainArgs()
        ));
        try {
            serverMain.start();
            return serverMain;
        } catch (Throwable th) {
            Misc.free(serverMain, th);
            throw th;
        }
    }

    private static String textResponse(String status, String message) {
        return "HTTP/1.1 " + status + "\r\n" +
                "Server: questDB/1.0\r\n" +
                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                "Transfer-Encoding: chunked\r\n" +
                "Content-Type: text/plain; charset=utf-8\r\n" +
                "\r\n" +
                chunk(message + "\r\n") +
                "00\r\n\r\n";
    }

    // url is the path with its query string, URL-encoded
    private record Endpoint(int port, String method, String url, String contentType, String body) {
        static Endpoint get(String url) {
            return new Endpoint(HTTP_PORT, "GET", url, null, null);
        }

        static Endpoint post(String url, String contentType, String body) {
            return new Endpoint(HTTP_PORT, "POST", url, contentType, body);
        }
    }
}
