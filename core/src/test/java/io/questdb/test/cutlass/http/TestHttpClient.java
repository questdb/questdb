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

import io.questdb.cutlass.http.client.ChunkedResponse;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;

public class TestHttpClient implements QuietCloseable {
    private final HttpClient httpClient = HttpClientFactory.newInstance();
    private final StringSink sink = new StringSink();

    public void assertGet(CharSequence expectedResponse, CharSequence sql) {
        assertGet("/query", expectedResponse, sql);
    }

    public void assertGet(CharSequence url, CharSequence expectedResponse, CharSequence sql) {
        try {
            sink.clear();
            toSink0(url, sql, sink);
            TestUtils.assertEquals(expectedResponse, sink);
        } finally {
            httpClient.disconnect();
        }
    }

    @Override
    public void close() {
        Misc.free(httpClient);
    }

    public void toSink(CharSequence url, CharSequence sql, CharSink sink) {
        try {
            toSink0(url, sql, sink);
        } finally {
            httpClient.disconnect();
        }
    }

    private void toSink0(CharSequence url, CharSequence sql, CharSink sink) {
        HttpClient.Request req = httpClient.newRequest();
        HttpClient.ResponseHeaders rsp = req
                .GET()
                .url(url)
                .query("query", sql)
                .send("localhost", 9001);

        rsp.await();
        ChunkedResponse chunkedResponse = rsp.getChunkedResponse();
        HttpClient.Chunk chunk;

        while ((chunk = chunkedResponse.recv()) != null) {
            Chars.utf8toUtf16(chunk.lo(), chunk.hi(), sink);
        }
    }
}
