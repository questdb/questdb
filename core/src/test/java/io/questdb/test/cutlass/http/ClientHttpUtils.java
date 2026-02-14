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

import io.questdb.client.cutlass.http.client.Fragment;
import io.questdb.client.cutlass.http.client.HttpClient;
import io.questdb.client.cutlass.http.client.Response;
import io.questdb.client.std.ThreadLocal;
import io.questdb.client.std.str.StringSink;
import io.questdb.client.std.str.Utf8s;
import io.questdb.test.tools.TestUtils;

import java.net.URLEncoder;

@SuppressWarnings("unused")
public final class ClientHttpUtils {

    private static final ThreadLocal<StringSink> tlSink = new ThreadLocal<>(StringSink::new);

    private ClientHttpUtils() {
    }

    public static void assertResponse(HttpClient.Request request, int expectedStatusCode, String expectedHttpResponse) {
        try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
            responseHeaders.await();
            TestUtils.assertEquals(String.valueOf(expectedStatusCode), responseHeaders.getStatusCode().asAsciiCharSequence());
            assertChunkedBody(responseHeaders, expectedHttpResponse);
        }
    }

    public static void assertChunkedBody(HttpClient.ResponseHeaders responseHeaders, String expectedBody) {
        StringSink sink = tlSink.get();
        sink.clear();
        Fragment fragment;
        final Response response = responseHeaders.getResponse();
        while ((fragment = response.recv()) != null) {
            Utf8s.utf8ToUtf16(fragment.lo(), fragment.hi(), sink);
        }
        TestUtils.assertEquals(expectedBody, sink);
    }

    public static void assertChunkedBodyContains(HttpClient.ResponseHeaders responseHeaders, String term) {
        StringSink sink = tlSink.get();
        sink.clear();
        Fragment fragment;
        final Response response = responseHeaders.getResponse();
        while ((fragment = response.recv()) != null) {
            Utf8s.utf8ToUtf16(fragment.lo(), fragment.hi(), sink);
        }
        TestUtils.assertContains(sink, term);
    }

    public static void awaitStatusCode(HttpClient.ResponseHeaders responseHeaders, CharSequence expectedStatusCode) {
        responseHeaders.await();
        TestUtils.assertEquals(expectedStatusCode, responseHeaders.getStatusCode().asAsciiCharSequence());
    }

    public static HttpClient.ResponseHeaders newHttpRequest(HttpClient client, int port, String query) {
        return newHttpRequest(client, port, query, "admin", "quest", null, null, null);
    }

    public static HttpClient.ResponseHeaders newHttpRequest(HttpClient client, int port, String query, String cookieName, String cookieValue) {
        return newHttpRequest(client, port, query, "admin", "quest", null, cookieName, cookieValue);
    }

    public static HttpClient.ResponseHeaders newHttpRequest(HttpClient client, int port, String query, String user, String pwd, String session) {
        return newHttpRequest(client, port, query, user, pwd, session, null, null);
    }

    public static HttpClient.ResponseHeaders newHttpRequest(HttpClient client, int port, String query, String user, String pwd, String session, String cookieName, String cookieValue) {
        HttpClient.Request r = client.newRequest("127.0.0.1", port);
        r = r.GET()
                .url("/exec")
                .query("query", query);
        if (session != null) {
            r.query("session", session);
        }
        if (user != null) {
            r.authBasic(user, pwd);
        }

        return r.send();
    }

    @SuppressWarnings("CharsetObjectCanBeUsed")
    public static String urlEncodeQuery(String query) {
        return TestUtils.unchecked(() -> URLEncoder.encode(query, "UTF8"));
    }
}
