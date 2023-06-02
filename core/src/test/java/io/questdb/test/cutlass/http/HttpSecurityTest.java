/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cutlass.http.HttpAuthenticator;
import io.questdb.cutlass.http.HttpAuthenticatorFactory;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Chars;
import io.questdb.test.AbstractTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;

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
    private static final String INVALID_CREDENTIALS_HEADER = "Authorization: Basic YmFyOmJheg=="; // bar:baz
    private static final String UNAUTHORIZED_RESPONSE = "HTTP/1.1 401 Unauthorized\r\n" +
            "Server: questDB/1.0\r\n" +
            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
            "Transfer-Encoding: chunked\r\n" +
            "Content-Type: text/plain; charset=utf-8\r\n" +
            "WWW-Authenticate: Basic realm=\"questdb\", charset=\"UTF-8\"\r\n";
    private static final String VALID_CREDENTIALS = "Basic Zm9vOmJhcg=="; // foo:bar
    private static final HttpAuthenticatorFactory SINGLE_USER_AUTH_FACTORY = () -> new HttpAuthenticator() {
        @Override
        public boolean authenticate(HttpRequestHeader headers) {
            return Chars.equalsNc(VALID_CREDENTIALS, headers.getHeader("Authorization"));
        }

        @Override
        public CharSequence getPrincipal() {
            return "foo";
        }
    };
    private static final String VALID_CREDENTIALS_HEADER = "Authorization: " + VALID_CREDENTIALS;

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(10 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Test
    public void testChkAllowWithValidCredentials() throws Exception {
        testHttpEndpoint(SINGLE_USER_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /chk?f=json&j=x HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        VALID_CREDENTIALS_HEADER + "\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "1b\r\n" +
                        "{\"status\":\"Does not exist\"}\r\n" +
                        "00\r\n" +
                        "\r\n"
        ));
    }

    @Test
    public void testChkDisallow() throws Exception {
        testHttpEndpoint(DENY_ALL_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /chk?f=json&j=x HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                UNAUTHORIZED_RESPONSE
        ));
    }

    @Test
    public void testChkDisallowWithInvalidCredentials() throws Exception {
        testHttpEndpoint(SINGLE_USER_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /chk?f=json&j=x HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        INVALID_CREDENTIALS_HEADER + "\r\n" +
                        "\r\n",
                UNAUTHORIZED_RESPONSE
        ));
    }

    @Test
    public void testExecAllowWithValidCredentials() throws Exception {
        testHttpEndpoint(SINGLE_USER_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /exec?query=" + HttpUtils.urlEncodeQuery("select 1") + "&count=true HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        VALID_CREDENTIALS_HEADER + "\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "63\r\n" +
                        "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"dataset\":[[1]],\"timestamp\":-1,\"count\":1}\r\n" +
                        "00\r\n" +
                        "\r\n"
        ));
    }

    @Test
    public void testExecDisallow() throws Exception {
        testHttpEndpoint(DENY_ALL_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /exec?query=" + HttpUtils.urlEncodeQuery("select 1") + "&count=true HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                UNAUTHORIZED_RESPONSE
        ));
    }

    @Test
    public void testExecDisallowInvalidCredentials() throws Exception {
        testHttpEndpoint(SINGLE_USER_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /exec?query=" + HttpUtils.urlEncodeQuery("select 1") + "&count=true HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        INVALID_CREDENTIALS_HEADER + "\r\n" +
                        "\r\n",
                UNAUTHORIZED_RESPONSE
        ));
    }

    @Test
    public void testExpAllowWithValidCredentials() throws Exception {
        testHttpEndpoint(SINGLE_USER_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /exp?query=" + HttpUtils.urlEncodeQuery("select 1") + "&count=true HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        VALID_CREDENTIALS_HEADER + "\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/csv; charset=utf-8\r\n" +
                        "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "08\r\n" +
                        "\"1\"\r\n" +
                        "1\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n"
        ));
    }

    @Test
    public void testExpDisallow() throws Exception {
        testHttpEndpoint(DENY_ALL_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /exp?query=" + HttpUtils.urlEncodeQuery("select 1") + "&count=true HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                UNAUTHORIZED_RESPONSE
        ));
    }

    @Test
    public void testExpDisallowWithInvalidCredentials() throws Exception {
        testHttpEndpoint(SINGLE_USER_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /exp?query=" + HttpUtils.urlEncodeQuery("select 1") + "&count=true HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        INVALID_CREDENTIALS_HEADER + "\r\n" +
                        "\r\n",
                UNAUTHORIZED_RESPONSE
        ));
    }

    @Test
    public void testHealthCheckAllowWithDisabledConfigProp() throws Exception {
        testAdditionalUnprotectedHttpEndpoint(SINGLE_USER_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /status HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain\r\n" +
                        "\r\n" +
                        "0f\r\n" +
                        "Status: Healthy\r\n" +
                        "00\r\n" +
                        "\r\n"
        ));
    }

    @Test
    public void testHealthCheckAllowWithValidCredentials() throws Exception {
        testHttpEndpoint(SINGLE_USER_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /status HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        VALID_CREDENTIALS_HEADER + "\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain\r\n" +
                        "\r\n" +
                        "0f\r\n" +
                        "Status: Healthy\r\n" +
                        "00\r\n" +
                        "\r\n"
        ));
    }

    @Test
    public void testHealthCheckDisallow() throws Exception {
        testHttpEndpoint(SINGLE_USER_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /status HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                UNAUTHORIZED_RESPONSE
        ));
    }

    @Test
    public void testImpAllowWithValidCredentials() throws Exception {
        testHttpEndpoint(SINGLE_USER_AUTH_FACTORY, engine -> sendAndReceive(
                "POST /upload?name=test HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "User-Agent: curl/7.71.1\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Length: 243\r\n" +
                        "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        VALID_CREDENTIALS_HEADER + "\r\n" +
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
        testHttpEndpoint(DENY_ALL_AUTH_FACTORY, engine -> sendAndReceive(
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
        testHttpEndpoint(SINGLE_USER_AUTH_FACTORY, engine -> sendAndReceive(
                "POST /upload?name=test HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "User-Agent: curl/7.71.1\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Length: 243\r\n" +
                        "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        INVALID_CREDENTIALS_HEADER + "\r\n" +
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
    public void testJsonQueryAllowWithValidCredentials() throws Exception {
        testHttpEndpoint(SINGLE_USER_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /query?query=" + HttpUtils.urlEncodeQuery("select 1") + "&count=true HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        VALID_CREDENTIALS_HEADER + "\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "63\r\n" +
                        "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"dataset\":[[1]],\"timestamp\":-1,\"count\":1}\r\n" +
                        "00\r\n" +
                        "\r\n"
        ));
    }

    @Test
    public void testJsonQueryDisallow() throws Exception {
        testHttpEndpoint(DENY_ALL_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /query?query=" + HttpUtils.urlEncodeQuery("select 1") + "&count=true HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                UNAUTHORIZED_RESPONSE
        ));
    }

    @Test
    public void testJsonQueryDisallowWithInvalidCredentials() throws Exception {
        testHttpEndpoint(SINGLE_USER_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /query?query=" + HttpUtils.urlEncodeQuery("select 1") + "&count=true HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        INVALID_CREDENTIALS_HEADER + "\r\n" +
                        "\r\n",
                UNAUTHORIZED_RESPONSE
        ));
    }

    @Test
    public void testStaticContentAllowWithDisabledConfigProp() throws Exception {
        testAdditionalUnprotectedHttpEndpoint(SINGLE_USER_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /index.html HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 404 Not Found\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "0b\r\n" +
                        "Not Found\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n"
        ));
    }

    @Test
    public void testStaticContentAllowWithValidCredentials() throws Exception {
        testHttpEndpoint(SINGLE_USER_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /index.html HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        VALID_CREDENTIALS_HEADER + "\r\n" +
                        "\r\n",
                "HTTP/1.1 404 Not Found\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "0b\r\n" +
                        "Not Found\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n"
        ));
    }

    @Test
    public void testStaticContentDisallow() throws Exception {
        testHttpEndpoint(DENY_ALL_AUTH_FACTORY, engine -> sendAndReceive(
                "GET /index.html HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                UNAUTHORIZED_RESPONSE
        ));
    }

    private static void sendAndReceive(String request, CharSequence response) {
        new SendAndReceiveRequestBuilder()
                .withNetworkFacade(NetworkFacadeImpl.INSTANCE)
                .execute(request, response);
    }

    private void testAdditionalUnprotectedHttpEndpoint(HttpAuthenticatorFactory factory, HttpQueryTestBuilder.HttpClientCode code) throws Exception {
        testHttpEndpoint(factory, false, false, code);
    }

    private void testHttpEndpoint(
            HttpAuthenticatorFactory factory,
            HttpQueryTestBuilder.HttpClientCode code
    ) throws Exception {
        testHttpEndpoint(factory, true, true, code);
    }

    private void testHttpEndpoint(
            HttpAuthenticatorFactory factory,
            boolean staticContentAuthRequired,
            boolean healthCheckAuthRequired,
            HttpQueryTestBuilder.HttpClientCode code
    ) throws Exception {
        final FactoryProvider factoryProvider = new DefaultFactoryProvider() {
            @Override
            public HttpAuthenticatorFactory getHttpAuthenticatorFactory() {
                return factory;
            }
        };
        new HttpQueryTestBuilder()
                .withWorkerCount(1)
                .withTempFolder(root)
                .withFactoryProvider(factoryProvider)
                .withStaticContentAuthRequired(staticContentAuthRequired)
                .withHealthCheckAuthRequired(healthCheckAuthRequired)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .run(code);
    }
}
