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
import io.questdb.cutlass.http.processors.TextImportProcessor;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.CharSinkBase;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import java.util.regex.Pattern;

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
            HttpClient.Request req = httpClient.newRequest();
            req
                    .GET("localhost", 9001)//TODO: empty call?
                    .url(url);

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
            Assert.assertTrue(message, pattern.matcher(sink).matches());
        } finally {
            if (!keepConnection) {
                httpClient.disconnect();
            }
        }
    }

    public void assertSendMultipart(
            CharSequence expectedResponse,
            @Nullable CharSequence json,
            CharSequence jsonVersion,
            CharSequence csv,
            CharSequence fileName,
            @Nullable CharSequence tableName,
            @Nullable CharSequence responseFormat,
            @Nullable CharSequence timestampColumnName,
            @Nullable CharSequence partitionBy,
            @Nullable CharSequence forceHeader,
            @Nullable CharSequence overwrite
    ) {
        sink.clear();
        try {
            toSinkImport0(
                    tableName,
                    json,
                    jsonVersion,
                    csv,
                    fileName,
                    responseFormat,
                    timestampColumnName,
                    partitionBy,
                    forceHeader,
                    overwrite,
                    null,
                    null,
                    sink
            );
            TestUtils.assertEquals(expectedResponse, sink);
        } finally {
            httpClient.disconnect();
        }
    }

    public void assertSendMultipart(
            CharSequence expectedResponse,
            @Nullable CharSequence json,
            CharSequence jsonVersion,
            CharSequence csv,
            CharSequence fileName,
            @Nullable CharSequence tableName,
            @Nullable CharSequence responseFormat,
            @Nullable CharSequence timestampColumnName,
            @Nullable CharSequence partitionBy,
            @Nullable CharSequence forceHeader,
            @Nullable CharSequence overwrite,
            @Nullable CharSequence skipLines,
            @Nullable CharSequence delimiter
    ) {
        sink.clear();
        try {
            toSinkImport0(
                    tableName,
                    json,
                    jsonVersion,
                    csv,
                    fileName,
                    responseFormat,
                    timestampColumnName,
                    partitionBy,
                    forceHeader,
                    overwrite,
                    skipLines,
                    delimiter,
                    sink
            );
            TestUtils.assertEquals(expectedResponse, sink);
        } finally {
            httpClient.disconnect();
        }
    }

    public void assertSendMultipart(
            CharSequence expectedResponse,
            @Nullable CharSequence json,
            CharSequence csv,
            CharSequence fileName,
            @Nullable CharSequence tableName,
            @Nullable CharSequence responseFormat,
            @Nullable CharSequence timestampColumnName,
            @Nullable CharSequence partitionBy,
            @Nullable CharSequence forceHeader,
            @Nullable CharSequence overwrite
    ) {
        assertSendMultipart(
                expectedResponse,
                json,
                TextImportProcessor.SCHEMA_V1,
                csv,
                fileName,
                tableName,
                responseFormat,
                timestampColumnName,
                partitionBy,
                forceHeader,
                overwrite
        );
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

    public StringSink getSink() {
        return sink;
    }

    public void setKeepConnection(boolean keepConnection) {
        this.keepConnection = keepConnection;
    }

    public void toSink(CharSequence url, CharSequence sql, StringSink sink) {
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
            StringSink sink,
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
                throw new RuntimeException("username specified without pwd or tooken");
            }
        }

        HttpClient.ResponseHeaders rsp = req.send("localhost", 9001);
        rsp.await();

        if (expectedStatus != null) {
            TestUtils.assertEquals(expectedStatus, rsp.getStatusCode());
        }

        ChunkedResponse chunkedResponse = rsp.getChunkedResponse();
        Chunk chunk;

        sink.clear();
        while ((chunk = chunkedResponse.recv()) != null) {
            Utf8s.utf8ToUtf16(chunk.lo(), chunk.hi(), sink);
        }
    }

    private void toSink0(
            CharSequence url,
            CharSequence sql,
            StringSink sink,
            @Nullable CharSequence username,
            @Nullable CharSequence password,
            @Nullable CharSequence token,
            CharSequenceObjHashMap<String> queryParams,
            CharSequence expectedStatus
    ) {
        HttpClient.Request req = httpClient.newRequest();
        req
                .GET()
                .url(url)
                .query("query", sql);

        reqToSink(req, sink, username, password, token, queryParams, expectedStatus);
    }

    private void toSinkImport0(
            CharSequence tableName,
            CharSequence json,
            CharSequence jsonVersion,
            CharSequence csv,
            CharSequence fileName,
            CharSequence responseFormat,
            CharSequence timestampColumnName,
            CharSequence partitionBy,
            CharSequence forceHeader,
            CharSequence overwrite,
            CharSequence skipLines,
            CharSequence delimiter,
            CharSinkBase<?> sink
    ) {
        HttpClient.Request req = httpClient.newRequest();
        req
                .POST("localhost", 9001)
                .url("/upload")
                .query("name", tableName)
                .query("fmt", responseFormat)
                .query("timestamp", timestampColumnName)
                .query("partitionBy", partitionBy)
                .query("overwrite", overwrite)
                .query("forceHeader", forceHeader)
                .query("skipLines", skipLines)
//                .query("skipLev", "false")
                .query("delimiter", delimiter)
//                .query("atomicitiy", "skipCol")
        ;

        HttpClient.MultipartRequest multipart = req.multipart();
        HttpClient.FormData data;

        // csv schema part has to be consumed before the csv itself
        // this part is optional
        if (json != null) {
            data = multipart.formData(jsonVersion);
            data.put(json);
        }

        data = multipart.formData("data", fileName);
        data.put(csv);

        HttpClient.Response rsp = multipart.send();
        rsp.await();
        ChunkedResponse chunkedResponse = rsp.getChunkedResponse();
        Chunk chunk;

        while ((chunk = chunkedResponse.recv()) != null) {
            Utf8s.utf8ToUtf16(chunk.lo(), chunk.hi(), sink);
        }
    }

}
