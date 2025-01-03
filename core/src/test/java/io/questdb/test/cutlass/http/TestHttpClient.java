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

import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import java.util.regex.Pattern;

public class TestHttpClient implements QuietCloseable {
    private final HttpClient httpClient;
    private final Utf8StringSink sink = new Utf8StringSink();
    private boolean keepConnection;

    public TestHttpClient() {
        httpClient = HttpClientFactory.newPlainTextInstance();
    }

    public TestHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public void assertGet(CharSequence expectedResponse, CharSequence sql) {
        assertGet(expectedResponse, sql, null, null);
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
            @Nullable CharSequence username,
            @Nullable CharSequence password
    ) {
        assertGet("/query", expectedResponse, sql, username, password);
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
            HttpClient.Request req = httpClient.newRequest("localhost", 9001);
            req.GET().url(url);

            if (queryParams != null) {
                for (int i = 0, n = queryParams.size(); i < n; i++) {
                    CharSequence name = queryParams.keys().getQuick(i);
                    req.query(name, queryParams.get(name));
                }
            }

            reqToSink(req, sink, username, password, token, null, null);
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
        Misc.free(httpClient);
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

    private void reqToSink(
            HttpClient.Request req,
            Utf8StringSink sink,
            @Nullable CharSequence username,
            @Nullable CharSequence password,
            @Nullable CharSequence token,
            CharSequenceObjHashMap<String> queryParams,
            CharSequence expectedStatus
    ) {
        if (queryParams != null) {
            for (int i = 0, n = queryParams.size(); i < n; i++) {
                CharSequence name = queryParams.keys().getQuick(i);
                req.query(name, queryParams.get(name));
            }
        }

        if (username != null) {
            if (password != null) {
                req.authBasic(username, password);
            } else if (token != null) {
                req.authToken(username, token);
            } else {
                throw new RuntimeException("username specified without pwd or token");
            }
        }

        HttpClient.ResponseHeaders rsp = req.send();
        rsp.await();

        if (expectedStatus != null) {
            TestUtils.assertEquals(expectedStatus, rsp.getStatusCode());
        }

        Response chunkedResponse = rsp.getResponse();
        Fragment fragment;

        sink.clear();
        while ((fragment = chunkedResponse.recv()) != null) {
            Utf8s.strCpy(fragment.lo(), fragment.hi(), sink);
        }
    }

    private void toSink0(
            CharSequence url,
            CharSequence sql,
            Utf8StringSink sink,
            @Nullable CharSequence username,
            @Nullable CharSequence password,
            @Nullable CharSequence token,
            CharSequenceObjHashMap<String> queryParams,
            CharSequence expectedStatus
    ) {
        HttpClient.Request req = httpClient.newRequest("localhost", 9001);
        req.GET().url(url).query("query", sql);

        reqToSink(req, sink, username, password, token, queryParams, expectedStatus);
    }
}
