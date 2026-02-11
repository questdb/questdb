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

import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.MutableUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import java.util.regex.Pattern;

public class TestHttpClient implements QuietCloseable {
    protected static final CharSequenceObjHashMap<String> PARQUET_GET_PARAM = new CharSequenceObjHashMap<>();
    private static final Log LOG = LogFactory.getLog(TestHttpClient.class);
    protected final Utf8StringSink sink = new Utf8StringSink();
    protected int port = 9001;
    private HttpClient httpClient;
    private boolean keepConnection;

    public TestHttpClient() {
        httpClient = HttpClientFactory.newPlainTextInstance();
    }

    public TestHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public void assertGet(CharSequence expectedResponse, CharSequence sql) {
        assertGet(expectedResponse, sql, "localhost", 9001, null, null);
    }

    public void assertGet(CharSequence expectedResponse, CharSequence sql, String host, int port) {
        assertGet(expectedResponse, sql, host, port, null, null);
    }

    public void assertGet(String host, int port, CharSequence expectedResponse, CharSequence sql, CharSequenceObjHashMap<String> queryParams) {
        try {
            toSink0(host, port, "/query", sql, sink, null, null, null, queryParams, null);
            TestUtils.assertEquals(expectedResponse, sink);
        } finally {
            if (!keepConnection) {
                httpClient.disconnect();
            }
        }
    }

    public void assertGet(CharSequence expectedResponse, CharSequence sql, CharSequenceObjHashMap<String> queryParams) {
        try {
            toSink0("/query", sql, sink, null, null, null, queryParams, null);
            TestUtils.assertEquals(expectedResponse, sink);
        } finally {
            if (!keepConnection) {
                httpClient.disconnect();
            }
        }
    }

    public void assertGet(
            CharSequence expectedResponse,
            CharSequence sql,
            String host,
            int port,
            @Nullable CharSequence username,
            @Nullable CharSequence password
    ) {
        assertGet("/query", expectedResponse, sql, host, port, username, password, null);
    }

    public void assertGet(
            CharSequence url,
            CharSequence expectedResponse,
            CharSequence sql
    ) {
        assertGet(url, expectedResponse, sql, null, null);
    }

    public void assertGet(
            CharSequence url,
            CharSequence expectedResponse,
            CharSequence sql,
            @Nullable CharSequenceObjHashMap<String> queryParams
    ) {
        try {
            toSink0(url, sql, sink, null, null, null, queryParams, null);
            TestUtils.assertEquals(expectedResponse, sink);
        } finally {
            if (!keepConnection) {
                httpClient.disconnect();
            }
        }
    }

    public void assertGet(
            CharSequence url,
            CharSequence expectedResponse,
            @Nullable CharSequenceObjHashMap<String> queryParams,
            @Nullable CharSequence username,
            @Nullable CharSequence password
    ) {
        assertGet(url, expectedResponse, queryParams, username, password, null);
    }

    public void assertGet(
            CharSequence url,
            CharSequence expectedResponse,
            @Nullable CharSequenceObjHashMap<String> queryParams,
            @Nullable CharSequence username,
            @Nullable CharSequence password,
            @Nullable CharSequence token
    ) {
        try {
            HttpClient.Request req = httpClient.newRequest("localhost", port);
            req.GET().url(url);

            if (queryParams != null) {
                for (int i = 0, n = queryParams.size(); i < n; i++) {
                    CharSequence name = queryParams.keys().getQuick(i);
                    req.query(name, queryParams.get(name));
                }
            }

            reqToSink(req, sink, username, password, token, null);
            try {
                TestUtils.assertEquals(expectedResponse, sink);
            } catch (AssertionError e) {
                LOG.info().$("=== ACTUAL RESULT IN \\u NOTATION ===").$();
                LOG.info().$(toUnicodeEscape(sink)).$();
                LOG.info().$("=== END ===").$();
                throw e;
            }
        } finally {
            if (!keepConnection) {
                httpClient.disconnect();
            }
        }
    }

    public void assertGet(
            CharSequence url,
            CharSequence expectedResponse,
            CharSequence sql,
            @Nullable CharSequence username,
            @Nullable CharSequence password
    ) {
        assertGet(url, expectedResponse, sql, username, password, null);
    }

    public void assertGet(
            CharSequence url,
            CharSequence expectedResponse,
            CharSequence sql,
            @Nullable CharSequence username,
            @Nullable CharSequence password,
            @Nullable CharSequence token
    ) {
        try {
            toSink0(url, sql, sink, username, password, token, null, null);
            TestUtils.assertEquals(expectedResponse, sink);
        } finally {
            if (!keepConnection) {
                httpClient.disconnect();
            }
        }
    }

    public void assertGet(
            CharSequence url,
            CharSequence expectedResponse,
            CharSequence sql,
            String host,
            int port,
            @Nullable CharSequence username,
            @Nullable CharSequence password,
            @Nullable CharSequence token
    ) {
        try {
            toSink0(host, port, url, sql, sink, username, password, token, null, null);
            TestUtils.assertEquals(expectedResponse, sink);
        } finally {
            if (!keepConnection) {
                httpClient.disconnect();
            }
        }
    }

    public void assertGet(
            CharSequence url,
            CharSequence expectedResponse,
            Utf8Sequence sql
    ) {
        assertGet(
                url,
                expectedResponse,
                sql,
                "127.0.0.1",
                9001,
                null,
                null,
                null,
                null
        );
    }

    public void assertGet(
            CharSequence url,
            CharSequence expectedResponse,
            Utf8Sequence sql,
            @Nullable CharSequenceObjHashMap<Utf8Sequence> queryParams
    ) {
        assertGet(
                url,
                expectedResponse,
                sql,
                "127.0.0.1",
                9001,
                null,
                null,
                null,
                queryParams
        );
    }

    public void assertGet(
            CharSequence url,
            CharSequence expectedResponse,
            Utf8Sequence sql,
            String host,
            int port,
            @Nullable CharSequence username,
            @Nullable CharSequence password,
            @Nullable CharSequence token,
            @Nullable CharSequenceObjHashMap<Utf8Sequence> queryParams
    ) {
        try {
            toSink0(host, port, url, sql, sink, username, password, token, queryParams);
            TestUtils.assertEquals(expectedResponse, sink);
        } finally {
            if (!keepConnection) {
                httpClient.disconnect();
            }
        }
    }

    public void assertGetContains(
            CharSequence url,
            CharSequence expectedResponse,
            @Nullable CharSequenceObjHashMap<String> queryParams
    ) {
        assertGetContains(url, expectedResponse, queryParams, null, null, 9001);
    }

    public void assertGetContains(
            CharSequence url,
            CharSequence expectedResponse,
            @Nullable CharSequenceObjHashMap<String> queryParams,
            @Nullable CharSequence username,
            @Nullable CharSequence password,
            int port
    ) {
        try {
            HttpClient.Request req = httpClient.newRequest("localhost", port);
            req.GET().url(url);

            if (queryParams != null) {
                for (int i = 0, n = queryParams.size(); i < n; i++) {
                    CharSequence name = queryParams.keys().getQuick(i);
                    req.query(name, queryParams.get(name));
                }
            }

            reqToSink(req, sink, username, password, null, null);
            TestUtils.assertContains(sink.toString(), expectedResponse);
        } finally {
            if (!keepConnection) {
                httpClient.disconnect();
            }
        }
    }

    public void assertGetParquet(
            CharSequence url,
            CharSequence expectedResponse,
            CharSequence sql
    ) {
        assertGetParquet(url, expectedResponse, sql, null, null);
    }

    public void assertGetParquet(
            int port,
            CharSequence url,
            CharSequence expectedResponseCode,
            int expectedResponseLength,
            CharSequence sql
    ) {
        try {
            this.port = port;
            toSink0(url, sql, sink, null, null, null, PARQUET_GET_PARAM, expectedResponseCode);
            Assert.assertEquals(expectedResponseLength, sink.size());
        } finally {
            if (!keepConnection) {
                httpClient.disconnect();
            }
        }
    }

    public void assertGetParquet(
            CharSequence url,
            int expectedResponseLength,
            CharSequence sql
    ) {
        try {
            toSink0(url, sql, sink, null, null, null, PARQUET_GET_PARAM, null);
            Assert.assertEquals(expectedResponseLength, sink.size());
        } finally {
            if (!keepConnection) {
                httpClient.disconnect();
            }
        }
    }

    public void assertGetParquet(
            CharSequence url,
            int expectedResponseLength,
            CharSequenceObjHashMap<String> param,
            CharSequence sql
    ) {
        try {
            toSink0(url, sql, sink, null, null, null, param, null);
            Assert.assertEquals(expectedResponseLength, sink.size());
        } finally {
            if (!keepConnection) {
                httpClient.disconnect();
            }
        }
    }

    public void assertGetParquet(
            CharSequence url,
            CharSequence expectedResponse,
            CharSequence sql,
            @Nullable CharSequence username,
            @Nullable CharSequence password
    ) {
        assertGetParquet(url, expectedResponse, sql, username, password, null);
    }

    public void assertGetParquet(
            CharSequence url,
            CharSequence expectedResponse,
            CharSequence sql,
            @Nullable CharSequence username,
            @Nullable CharSequence password,
            @Nullable CharSequence token
    ) {
        try {
            toSink0(url, sql, sink, username, password, token, PARQUET_GET_PARAM, null);
            TestUtils.assertEquals(expectedResponse, sink);
        } finally {
            if (!keepConnection) {
                httpClient.disconnect();
            }
        }
    }

    public void assertGetRegexp(
            CharSequence url,
            String expectedResponseRegexp,
            CharSequence sql,
            @Nullable CharSequence username,
            @Nullable CharSequence password,
            @Nullable CharSequence token,
            @Nullable CharSequenceObjHashMap<String> queryParams,
            CharSequence expectedStatus
    ) {
        try {
            toSink0(url, sql, sink, username, password, token, queryParams, expectedStatus);
            Pattern pattern = Pattern.compile(expectedResponseRegexp);
            String message = "Expected response to match regexp " + expectedResponseRegexp + " but got " + sink + " which does not match";
            Assert.assertTrue(message, pattern.matcher(sink.toString()).matches());
        } finally {
            if (!keepConnection) {
                httpClient.disconnect();
            }
        }
    }

    public void assertGetRegexp(
            CharSequence url,
            String expectedResponseRegexp,
            CharSequence sql,
            @Nullable CharSequence username,
            @Nullable CharSequence password,
            CharSequence expectedStatus
    ) {
        assertGetRegexp(url, expectedResponseRegexp, sql, username, password, expectedStatus, null, null);
    }

    @Override
    public void close() {
        httpClient = Misc.free(httpClient);
    }

    public void disconnect() {
        httpClient.disconnect();
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }

    public Utf8StringSink getSink() {
        return sink;
    }

    public void setKeepConnection(boolean keepConnection) {
        this.keepConnection = keepConnection;
    }

    public void toSink(CharSequence url, CharSequence sql, Utf8StringSink sink) {
        try {
            toSink0(url, sql, sink, null, null, null, null, null);
        } finally {
            if (!keepConnection) {
                httpClient.disconnect();
            }
        }
    }

    private static String toUnicodeEscape(Utf8StringSink sink) {
        // Convert UTF-8 to UTF-16 first (same as assertion does)
        io.questdb.std.str.StringSink utf16 = new io.questdb.std.str.StringSink();
        Utf8s.utf8ToUtf16(sink, utf16);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < utf16.length(); i++) {
            char c = utf16.charAt(i);
            if (c >= 32 && c < 127 && c != '\\' && c != '"') {
                sb.append(c);
            } else if (c == '\\') {
                sb.append("\\\\");
            } else if (c == '"') {
                sb.append("\\\"");
            } else if (c == '\b') {
                sb.append("\\b");
            } else if (c == '\t') {
                sb.append("\\t");
            } else if (c == '\n') {
                sb.append("\\n");
            } else if (c == '\f') {
                sb.append("\\f");
            } else if (c == '\r') {
                sb.append("\\r");
            } else {
                sb.append(String.format("\\u%04x", (int) c));
            }
        }
        return sb.toString();
    }

    protected String reqToSink(
            HttpClient.Request req,
            MutableUtf8Sink sink,
            @Nullable CharSequence username,
            @Nullable CharSequence password,
            @Nullable CharSequence token,
            CharSequenceObjHashMap<String> queryParams
    ) {
        if (queryParams != null) {
            for (int i = 0, n = queryParams.size(); i < n; i++) {
                CharSequence name = queryParams.keys().getQuick(i);
                req.query(name, queryParams.get(name));
            }
        }

        return reqToSink0(req, sink, username, password, token);
    }

    protected String reqToSink0(
            HttpClient.Request req,
            MutableUtf8Sink sink,
            @Nullable CharSequence username,
            @Nullable CharSequence password,
            @Nullable CharSequence token
    ) {
        if (username != null) {
            if (password != null) {
                req.authBasic(username, password);
            } else if (token != null) {
                req.authToken(username, token);
            } else {
                throw new RuntimeException("username specified without pwd or token");
            }
        }

        @SuppressWarnings("resource") HttpClient.ResponseHeaders rsp = req.send();
        rsp.await();

        String statusCode = Utf8s.toString(rsp.getStatusCode());
        sink.clear();
        rsp.getResponse().copyTextTo(sink);
        return statusCode;
    }

    protected void reqToSinkUtf8Params(
            HttpClient.Request req,
            MutableUtf8Sink sink,
            @Nullable CharSequence username,
            @Nullable CharSequence password,
            @Nullable CharSequence token,
            CharSequenceObjHashMap<Utf8Sequence> queryParams
    ) {
        if (queryParams != null) {
            for (int i = 0, n = queryParams.size(); i < n; i++) {
                CharSequence name = queryParams.keys().getQuick(i);
                req.query(name, queryParams.get(name));
            }
        }

        reqToSink0(req, sink, username, password, token);
    }

    protected void toSink0(
            CharSequence url,
            CharSequence sql,
            Utf8StringSink sink,
            @Nullable CharSequence username,
            @Nullable CharSequence password,
            @Nullable CharSequence token,
            CharSequenceObjHashMap<String> queryParams,
            CharSequence expectedStatus
    ) {
        toSink0(
                "localhost",
                port,
                url,
                sql,
                sink,
                username,
                password,
                token,
                queryParams,
                expectedStatus
        );
    }

    protected void toSink0(
            String host,
            int port,
            CharSequence url,
            CharSequence sql,
            Utf8StringSink sink,
            @Nullable CharSequence username,
            @Nullable CharSequence password,
            @Nullable CharSequence token,
            CharSequenceObjHashMap<String> queryParams,
            CharSequence expectedStatus
    ) {
        HttpClient.Request req = httpClient.newRequest(host, port);
        req.GET().url(url).query("query", sql);
        String respCode = reqToSink(req, sink, username, password, token, queryParams);
        if (expectedStatus != null) {
            if (!expectedStatus.equals(respCode)) {
                LOG.error().$("unexpected status code received, expected ").$(expectedStatus)
                        .$(", but was ").$(respCode).$(". Response: ").$safe(sink).$();
                Assert.fail("expected response code " + expectedStatus + " but got " + respCode);
            }
        }
    }

    protected void toSink0(
            String host,
            int port,
            CharSequence url,
            Utf8Sequence sql,
            Utf8StringSink sink,
            @Nullable CharSequence username,
            @Nullable CharSequence password,
            @Nullable CharSequence token,
            CharSequenceObjHashMap<Utf8Sequence> queryParams
    ) {
        HttpClient.Request req = httpClient.newRequest(host, port);
        req.GET().url(url).query("query", sql);
        reqToSinkUtf8Params(req, sink, username, password, token, queryParams);
    }

    static {
        PARQUET_GET_PARAM.put("fmt", "parquet");
    }
}
