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

import io.questdb.cutlass.http.client.Chunk;
import io.questdb.cutlass.http.client.ChunkedResponse;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;

public class TestHttpClient implements QuietCloseable {
    private final HttpClient httpClient = HttpClientFactory.newInstance();
    private final StringSink sink = new StringSink();
    private boolean keepConnection;

    public void assertGet(CharSequence expectedResponse, CharSequence sql) {
        assertGet(expectedResponse, sql, null, null);
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
            CharSequence username,
            CharSequence password
    ) {
        try {
            HttpClient.Request req = httpClient.newRequest();
            req
                    .GET("localhost", 9001)
                    .url(url);

            if (queryParams != null) {
                for (int i = 0, n = queryParams.size(); i < n; i++) {
                    CharSequence name = queryParams.keys().getQuick(i);
                    req.query(name, queryParams.get(name));
                }
            }

            if (username != null && password != null) {
                req.authBasic(username, password);
            }

            HttpClient.Response rsp = req.send();

            rsp.await();
            ChunkedResponse chunkedResponse = rsp.getChunkedResponse();
            Chunk chunk;

            sink.clear();
            while ((chunk = chunkedResponse.recv()) != null) {
                Chars.utf8toUtf16(chunk.lo(), chunk.hi(), sink);
            }

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
        try {
            sink.clear();
            toSinkGet0(url, sql, sink, username, password);
            TestUtils.assertEquals(expectedResponse, sink);
        } finally {
            if (!keepConnection) {
                httpClient.disconnect();
            }
        }
    }

    @Override
    public void close() {
        Misc.free(httpClient);
    }

    public void setKeepConnection(boolean keepConnection) {
        this.keepConnection = keepConnection;
    }

    public void toSink(CharSequence url, CharSequence sql, CharSink sink) {
        try {
            toSinkGet0(url, sql, sink, null, null);
        } finally {
            if (!keepConnection) {
                httpClient.disconnect();
            }
        }
    }

    private void toSinkGet0(
            CharSequence url,
            CharSequence sql,
            CharSink sink,
            @Nullable CharSequence username,
            @Nullable CharSequence password
    ) {
        HttpClient.Request req = httpClient.newRequest();
        req
                .GET("localhost", 9001)
                .url(url)
                .query("query", sql);

        if (username != null && password != null) {
            req.authBasic(username, password);
        }

        HttpClient.Response rsp = req.send();

        rsp.await();
        ChunkedResponse chunkedResponse = rsp.getChunkedResponse();
        Chunk chunk;

        while ((chunk = chunkedResponse.recv()) != null) {
            Chars.utf8toUtf16(chunk.lo(), chunk.hi(), sink);
        }
    }

    private void toSinkImport0(
            CharSequence url,
            CharSequence tableName,
            CharSequence csv,
            CharSink sink,
            @Nullable CharSequence username,
            @Nullable CharSequence password
    ) {
        HttpClient.Request req = httpClient.newRequest();
        req
                .POST("localhost", 9001)
                .url(url)
                .query("name", tableName)
                .query("partitionBy", "NONE")
                .query("overwrite", "false")
                .query("skipLev", "false")
                .query("delimiter", "")
                .query("atomicitiy", "skipCol")
        ;

        if (username != null && password != null) {
            req.authBasic(username, password);
        }
        HttpClient.MultipartRequest multipart = req.multipart();
        HttpClient.FormData data = multipart.formData("data");
        data.put(csv);
        HttpClient.Response rsp = multipart.send();

        rsp.await();
        ChunkedResponse chunkedResponse = rsp.getChunkedResponse();
        Chunk chunk;

        while ((chunk = chunkedResponse.recv()) != null) {
            Chars.utf8toUtf16(chunk.lo(), chunk.hi(), sink);
        }
    }
}
