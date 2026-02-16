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

import io.questdb.cutlass.http.HttpCookie;
import io.questdb.cutlass.http.HttpSessionStore;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.tools.TestUtils;

import java.net.URLEncoder;

import static io.questdb.cutlass.http.HttpConstants.*;
import static org.junit.Assert.*;

public final class HttpUtils {

    private static final ThreadLocal<StringSink> tlSink = new ThreadLocal<>(StringSink::new);

    private HttpUtils() {
    }

    public static void assertChunkedBody(HttpClient.ResponseHeaders responseHeaders, String expectedBody) {
        StringSink sink = tlSink.get();
        sink.clear();
        responseHeaders.getResponse().copyTextTo(sink);
        TestUtils.assertEquals(expectedBody, sink);
    }

    public static void assertChunkedBodyContains(HttpClient.ResponseHeaders responseHeaders, String term) {
        StringSink sink = tlSink.get();
        sink.clear();
        responseHeaders.getResponse().copyTextTo(sink);
        TestUtils.assertContains(sink, term);
    }

    public static void assertNoSessionCookie(HttpClient.ResponseHeaders responseHeaders) {
        final HttpCookie sessionCookie = responseHeaders.getCookie(SESSION_COOKIE_NAME_UTF8);
        assertNull(sessionCookie);
    }

    public static HttpSessionStore.SessionInfo assertSession(HttpSessionStore sessionStore, String sessionId, long currentTime, long sessionTimeout) {
        final HttpSessionStore.SessionInfo session = sessionStore.getSession(sessionId);
        assertNotNull(session);
        assertEquals(currentTime + sessionTimeout, session.getExpiresAt());
        assertEquals(currentTime + sessionTimeout / 2, session.getRotateAt());
        assertEquals("admin", session.getPrincipal());
        assertEquals(1, session.getAuthType());
        assertEquals(0, session.getGroups().size());
        return session;
    }

    public static String assertSessionCookie(HttpClient.ResponseHeaders responseHeaders, boolean secure) {
        final HttpCookie sessionCookie = responseHeaders.getCookie(SESSION_COOKIE_NAME_UTF8);
        assertNotNull(sessionCookie);
        assertEquals(SESSION_COOKIE_NAME, sessionCookie.cookieName.toString());
        assertEquals(secure, sessionCookie.secure);
        assertTrue(sessionCookie.httpOnly);
        assertEquals("/", sessionCookie.path.toString());
        assertEquals(-1L, sessionCookie.expires);
        assertEquals(SESSION_COOKIE_MAX_AGE_SECONDS, sessionCookie.maxAge);
        return sessionCookie.value.toString();
    }

    public static void assertSessionCookieDeleted(HttpClient.ResponseHeaders responseHeaders) {
        final HttpCookie sessionCookie = responseHeaders.getCookie(SESSION_COOKIE_NAME_UTF8);
        assertNotNull(sessionCookie);
        assertEquals(SESSION_COOKIE_NAME, sessionCookie.cookieName.toString());
        assertEquals(0L, sessionCookie.expires);
        assertEquals(0L, sessionCookie.maxAge);
        assertTrue(Utf8s.equalsUtf16("", sessionCookie.value));
    }

    public static void awaitStatusCode(HttpClient.ResponseHeaders responseHeaders, CharSequence expectedStatusCode) {
        responseHeaders.await();
        TestUtils.assertEquals(expectedStatusCode, responseHeaders.getStatusCode());
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

        if (cookieName != null) {
            r = r.setCookie(cookieName, cookieValue);
        }
        return r.send();
    }

    @SuppressWarnings("CharsetObjectCanBeUsed")
    public static String urlEncodeQuery(String query) {
        return TestUtils.unchecked(() -> URLEncoder.encode(query, "UTF8"));
    }
}
