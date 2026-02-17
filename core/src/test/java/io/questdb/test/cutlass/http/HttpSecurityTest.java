/*******************************************************************************
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

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.HttpAuthenticator;
import io.questdb.cutlass.http.HttpAuthenticatorFactory;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.StaticHttpAuthenticatorFactory;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Test;

public class HttpSecurityTest extends AbstractTest {

    private static final HttpAuthenticatorFactory DENY_ALL_AUTH_FACTORY = () -> new HttpAuthenticator() {
        @Override
        public boolean authenticate(HttpRequestHeader headers) {
            return false;
        }

        @Override
        public CharSequence getPrincipal() {
            return null;
        }
    };
    private static final String INVALID_BASIC_AUTH_CREDENTIALS_HEADER = "Authorization: Basic YmFyOmJheg=="; // bar:baz
    private static final String UNAUTHORIZED_RESPONSE = "HTTP/1.1 401 Unauthorized\r\n" +
            "Server: questDB/1.0\r\n" +
            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
            "Transfer-Encoding: chunked\r\n" +
            "Content-Type: text/plain; charset=utf-8\r\n" +
            "\r\n" +
            "0e\r\n" +
            "Unauthorized\r\n" +
            "\r\n" +
            "00\r\n" +
            "\r\n";
    private static final String VALID_BASIC_AUTH_CREDENTIALS = "Basic Zm9vOmJhcg=="; // foo:bar
    private static final HttpAuthenticatorFactory SINGLE_USER_BASIC_AUTH_FACTORY = () -> new HttpAuthenticator() {
        @Override
        public boolean authenticate(HttpRequestHeader headers) {
            return Utf8s.equalsNcAscii(VALID_BASIC_AUTH_CREDENTIALS, headers.getHeader(new Utf8String("Authorization")));
        }

        @Override
        public CharSequence getPrincipal() {
            return "foo";
        }
    };
    private static final String VALID_BASIC_AUTH_CREDENTIALS_HEADER = "Authorization: " + VALID_BASIC_AUTH_CREDENTIALS;
    private static final String VALID_BASIC_AUTH_CREDENTIALS_HEADER_RANDOM_CASE = "aUThOriZATiOn: " + VALID_BASIC_AUTH_CREDENTIALS;
    private static final String VALID_REST_TOKEN_AUTH_CREDENTIALS = "Bearer validToken-XubtaE";
    private static final HttpAuthenticatorFactory SINGLE_USER_REST_TOKEN_AUTH_FACTORY = () -> new HttpAuthenticator() {
        @Override
        public boolean authenticate(HttpRequestHeader headers) {
            return Utf8s.equalsNcAscii(VALID_REST_TOKEN_AUTH_CREDENTIALS, headers.getHeader(new Utf8String("Authorization")));
        }

        @Override
        public CharSequence getPrincipal() {
            return "foo";
        }
    };
    private static final TestHttpClient testHttpClient = new TestHttpClient();

    @AfterClass
    public static void tearDownStatic() {
        testHttpClient.close();
        AbstractTest.tearDownStatic();
        assert Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN) == 0;
    }

    @Test
    public void testChkAllowWithValidCredentials() throws Exception {
        testHttpEndpoint(
                SINGLE_USER_BASIC_AUTH_FACTORY,
                (engine, sqlExecutionContext) -> testHttpClient.assertGet(
                        "/chk",
                        "{\"status\":\"Does not exist\"}",
                        new CharSequenceObjHashMap<>() {{
                            put("f", "json");
                            put("j", "x");
                        }},
                        "foo",
                        "bar"
                )
        );
    }

    @Test
    public void testChkDisallow() throws Exception {
        testHttpEndpoint(
                DENY_ALL_AUTH_FACTORY,
                (engine, sqlExecutionContext) ->
                        testHttpClient.assertGet(
                                "/chk",
                                "Unauthorized\r\n",
                                new CharSequenceObjHashMap<>() {{
                                    put("f", "json");
                                    put("j", "x");
                                }},
                                null,
                                null
                        )
        );
    }

    @Test
    public void testChkDisallowWithInvalidCredentials() throws Exception {
        testHttpEndpoint(
                SINGLE_USER_BASIC_AUTH_FACTORY,
                (engine, sqlExecutionContext) -> testHttpClient.assertGet(
                        "/chk",
                        "Unauthorized\r\n",
                        new CharSequenceObjHashMap<>() {{
                            put("f", "json");
                            put("j", "x");
                        }},
                        "foo",
                        "baz"
                )
        );
    }

    @Test
    public void testExecAllowWithValidCredentials() throws Exception {
        testHttpEndpoint(
                SINGLE_USER_BASIC_AUTH_FACTORY,
                (engine, sqlExecutionContext) -> testHttpClient.assertGet(
                        "/exec",
                        "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                        "select 1",
                        "foo",
                        "bar"
                )
        );
    }

    @Test
    public void testExecDisallow() throws Exception {
        testHttpEndpoint(
                DENY_ALL_AUTH_FACTORY,
                (engine, sqlExecutionContext) -> testHttpClient.assertGet(
                        "/exec",
                        "Unauthorized\r\n",
                        "select 1",
                        "foo",
                        "baz"
                )
        );
    }

    @Test
    public void testExecDisallowInvalidCredentials() throws Exception {
        testHttpEndpoint(
                SINGLE_USER_BASIC_AUTH_FACTORY,
                (engine, sqlExecutionContext) -> testHttpClient.assertGet(
                        "/exec",
                        "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                        "select 1",
                        "foo",
                        "bar"
                )
        );
    }

    @Test
    public void testExpAllowWithValidCredentials() throws Exception {
        testHttpEndpoint(
                SINGLE_USER_BASIC_AUTH_FACTORY,
                (engine, sqlExecutionContext) -> testHttpClient.assertGet(
                        "/exp",
                        "\"1\"\r\n" +
                                "1\r\n",
                        "select 1",
                        "foo",
                        "bar"
                )
        );
    }

    @Test
    public void testExpDisallow() throws Exception {
        testHttpEndpoint(
                DENY_ALL_AUTH_FACTORY,
                (engine, sqlExecutionContext) -> testHttpClient.assertGet("Unauthorized\r\n", "select 1")
        );
    }

    @Test
    public void testExpDisallowWithInvalidCredentials2() throws Exception {
        testHttpEndpoint(
                SINGLE_USER_BASIC_AUTH_FACTORY,
                (engine, sqlExecutionContext) -> testHttpClient.assertGet(
                        "/exp",
                        "Unauthorized\r\n",
                        "select 1",
                        "foo",
                        "baz"
                )
        );
    }

    @Test
    public void testHealthCheckAllowWithDisabledConfigProp() throws Exception {
        testAdditionalUnprotectedHttpEndpoint(
                (engine, sqlExecutionContext) -> testHttpClient.assertGet(
                        "/status",
                        "Status: Healthy",
                        (CharSequenceObjHashMap<String>) null,
                        null,
                        null
                )
        );
    }

    @Test
    public void testHealthCheckAllowWithValidCredentials() throws Exception {
        testHttpEndpoint(
                SINGLE_USER_BASIC_AUTH_FACTORY,
                (engine, sqlExecutionContext) ->
                        testHttpClient.assertGet(
                                "/status",
                                "Status: Healthy",
                                (CharSequenceObjHashMap<String>) null,
                                "foo",
                                "bar"
                        )
        );
    }

    @Test
    public void testHealthCheckDisallow() throws Exception {
        testHttpEndpoint(
                SINGLE_USER_BASIC_AUTH_FACTORY,
                (engine, sqlExecutionContext) -> testHttpClient.assertGet(
                        "/status",
                        "Unauthorized\r\n",
                        (CharSequenceObjHashMap<String>) null,
                        null,
                        null
                )
        );
    }

    @Test
    public void testImpAllowWithValidCredentials() throws Exception {
        testHttpEndpoint(SINGLE_USER_BASIC_AUTH_FACTORY, (engine, sqlExecutionContext) -> sendAndReceive(
                "POST /upload?name=test HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "User-Agent: curl/7.71.1\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Length: 243\r\n" +
                        "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        VALID_BASIC_AUTH_CREDENTIALS_HEADER + "\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"data\"\r\n" +
                        "\r\n" +
                        "col_a,ts\r\n" +
                        "1000,1000\r\n" +
                        "2000,2000\r\n" +
                        "3000,3000\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "0507\r\n" +
                        "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                        "|      Location:  |                                              test  |        Pattern  | Locale  |      Errors  |\r\n" +
                        "|   Partition by  |                                              NONE  |                 |         |              |\r\n" +
                        "|      Timestamp  |                                              NONE  |                 |         |              |\r\n" +
                        "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                        "|   Rows handled  |                                                 3  |                 |         |              |\r\n" +
                        "|  Rows imported  |                                                 3  |                 |         |              |\r\n" +
                        "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                        "|              0  |                                             col_a  |                      INT  |           0  |\r\n" +
                        "|              1  |                                                ts  |                      INT  |           0  |\r\n" +
                        "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n"
        ));
    }

    @Test
    public void testImpDisallow() throws Exception {
        testHttpEndpoint(DENY_ALL_AUTH_FACTORY, (engine, sqlExecutionContext) -> sendAndReceive(
                "POST /upload?name=test HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "User-Agent: curl/7.71.1\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Length: 243\r\n" +
                        "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"data\"\r\n" +
                        "\r\n" +
                        "col_a,ts\r\n" +
                        "1000,1000\r\n" +
                        "2000,2000\r\n" +
                        "3000,3000\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                UNAUTHORIZED_RESPONSE
        ));
    }

    @Test
    public void testImpDisallowWithInvalidCredentials() throws Exception {
        testHttpEndpoint(SINGLE_USER_BASIC_AUTH_FACTORY, (engine, sqlExecutionContext) -> sendAndReceive(
                "POST /upload?name=test HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "User-Agent: curl/7.71.1\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Length: 243\r\n" +
                        "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        INVALID_BASIC_AUTH_CREDENTIALS_HEADER + "\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"data\"\r\n" +
                        "\r\n" +
                        "col_a,ts\r\n" +
                        "1000,1000\r\n" +
                        "2000,2000\r\n" +
                        "3000,3000\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                UNAUTHORIZED_RESPONSE
        ));
    }

    @Test
    public void testImplCheckAuthorizationHeaderIsCaseInsensitive() throws Exception {
        testHttpEndpoint(SINGLE_USER_BASIC_AUTH_FACTORY, (engine, sqlExecutionContext) -> sendAndReceive(
                "POST /upload?name=test HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "User-Agent: curl/7.71.1\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Length: 243\r\n" +
                        "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        VALID_BASIC_AUTH_CREDENTIALS_HEADER_RANDOM_CASE + "\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"data\"\r\n" +
                        "\r\n" +
                        "col_a,ts\r\n" +
                        "1000,1000\r\n" +
                        "2000,2000\r\n" +
                        "3000,3000\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "0507\r\n" +
                        "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                        "|      Location:  |                                              test  |        Pattern  | Locale  |      Errors  |\r\n" +
                        "|   Partition by  |                                              NONE  |                 |         |              |\r\n" +
                        "|      Timestamp  |                                              NONE  |                 |         |              |\r\n" +
                        "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                        "|   Rows handled  |                                                 3  |                 |         |              |\r\n" +
                        "|  Rows imported  |                                                 3  |                 |         |              |\r\n" +
                        "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                        "|              0  |                                             col_a  |                      INT  |           0  |\r\n" +
                        "|              1  |                                                ts  |                      INT  |           0  |\r\n" +
                        "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n"
        ));
    }

    @Test
    public void testJsonQueryAllowWithValidCredentials() throws Exception {
        testHttpEndpoint(
                SINGLE_USER_BASIC_AUTH_FACTORY,
                (engine, sqlExecutionContext) -> testHttpClient.assertGet(
                        "/query",
                        "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                        "select 1",
                        "foo",
                        "bar"
                )
        );
    }

    @Test
    public void testJsonQueryAllowWithValidToken() throws Exception {
        testHttpEndpoint(
                SINGLE_USER_REST_TOKEN_AUTH_FACTORY,
                (engine, sqlExecutionContext) -> testHttpClient.assertGet(
                        "/query",
                        "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                        "select 1",
                        "foo",
                        null,
                        "validToken-XubtaE"
                )
        );
    }

    @Test
    public void testJsonQueryDisallow() throws Exception {
        testHttpEndpoint(
                DENY_ALL_AUTH_FACTORY,
                (engine, sqlExecutionContext) -> testHttpClient.assertGet("Unauthorized\r\n", "select 1")
        );
    }

    @Test
    public void testJsonQueryDisallowWithInvalidCredentials() throws Exception {
        testHttpEndpoint(
                SINGLE_USER_BASIC_AUTH_FACTORY,
                (engine, sqlExecutionContext) -> testHttpClient.assertGet(
                        "/query",
                        "Unauthorized\r\n",
                        "select 1",
                        "bar",
                        "baz"
                )
        );
    }

    @Test
    public void testJsonQueryDisallowWithInvalidToken() throws Exception {
        testHttpEndpoint(
                SINGLE_USER_BASIC_AUTH_FACTORY,
                (engine, sqlExecutionContext) -> testHttpClient.assertGet(
                        "/query",
                        "Unauthorized\r\n",
                        "select 1",
                        "bar",
                        null,
                        "invalidToken-XubtaE"
                )
        );
    }

    @Test
    public void testStaticContentAllowWithDisabledConfigProp() throws Exception {
        testAdditionalUnprotectedHttpEndpoint(
                (engine, sqlExecutionContext) -> testHttpClient.assertGet(
                        "/index.html",
                        "Not Found\r\n",
                        (CharSequenceObjHashMap<String>) null,
                        null,
                        null
                )
        );
    }

    @Test
    public void testStaticContentAllowWithValidCredentials() throws Exception {
        testHttpEndpoint(
                SINGLE_USER_BASIC_AUTH_FACTORY,
                (engine, sqlExecutionContext) ->
                        testHttpClient.assertGet(
                                "/index.html",
                                "Not Found\r\n",
                                (CharSequenceObjHashMap<String>) null,
                                "foo",
                                "bar"
                        )
        );
    }

    @Test
    public void testStaticContentDisallow() throws Exception {
        testHttpEndpoint(
                DENY_ALL_AUTH_FACTORY,
                (engine, sqlExecutionContext) ->
                        testHttpClient.assertGet(
                                "/index.html",
                                "Unauthorized\r\n",
                                (CharSequenceObjHashMap<String>) null,
                                null,
                                null
                        )
        );
    }

    @Test
    public void testStaticHttpAuthenticatorFactory_badPassword() throws Exception {
        StaticHttpAuthenticatorFactory factory = new StaticHttpAuthenticatorFactory("foo", "bar");
        testHttpEndpoint(factory, SecurityContext.AUTH_TYPE_CREDENTIALS, SecurityContext.AUTH_TYPE_CREDENTIALS, (code, sqlExecutionContext) ->
                testHttpClient.assertGet(
                        "/query",
                        "Unauthorized\r\n",
                        "select 1",
                        "foo",
                        "notbar"
                )
        );
    }

    @Test
    public void testStaticHttpAuthenticatorFactory_success() throws Exception {
        StaticHttpAuthenticatorFactory factory = new StaticHttpAuthenticatorFactory("foo", "bar");
        testHttpEndpoint(factory, SecurityContext.AUTH_TYPE_CREDENTIALS, SecurityContext.AUTH_TYPE_CREDENTIALS, (code, sqlExecutionContext) ->
                testHttpClient.assertGet(
                        "/query",
                        "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                        "select 1",
                        "foo",
                        "bar"
                )
        );
    }

    private static void sendAndReceive(String request, CharSequence response) {
        new SendAndReceiveRequestBuilder()
                .withNetworkFacade(NetworkFacadeImpl.INSTANCE)
                .execute(request, response);
    }

    private void testAdditionalUnprotectedHttpEndpoint(HttpQueryTestBuilder.HttpClientCode code) throws Exception {
        testHttpEndpoint(HttpSecurityTest.SINGLE_USER_BASIC_AUTH_FACTORY, SecurityContext.AUTH_TYPE_NONE, SecurityContext.AUTH_TYPE_NONE, code);
    }

    private void testHttpEndpoint(
            HttpAuthenticatorFactory factory,
            HttpQueryTestBuilder.HttpClientCode code
    ) throws Exception {
        testHttpEndpoint(factory, SecurityContext.AUTH_TYPE_CREDENTIALS, SecurityContext.AUTH_TYPE_CREDENTIALS, code);
    }

    private void testHttpEndpoint(
            HttpAuthenticatorFactory factory,
            byte httpStaticContentAuthType,
            byte httpHealthCheckAuthType,
            HttpQueryTestBuilder.HttpClientCode code
    ) throws Exception {
        final FactoryProvider factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull HttpAuthenticatorFactory getHttpAuthenticatorFactory() {
                return factory;
            }
        };
        new HttpQueryTestBuilder()
                .withWorkerCount(1)
                .withTempFolder(root)
                .withFactoryProvider(factoryProvider)
                .withStaticContentAuthRequired(httpStaticContentAuthType)
                .withHealthCheckAuthRequired(httpHealthCheckAuthType)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .run(code);
    }
}
