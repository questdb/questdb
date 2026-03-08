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

package io.questdb.compat;

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.ServerMain;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import okhttp3.*;
import okio.BufferedSink;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static io.questdb.PropertyKey.*;

public class LineOkHttpFuzzTest extends AbstractTest {
    @Test
    public void testChunkedDataIlpUploadNoKeepAlive() throws Exception {
        Rnd rnd = generateRandom(LOG);
        int fragmentation = 10 + rnd.nextInt(5);
        LOG.info().$("=== fragmentation=").$(fragmentation).$();
        try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
            put(DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), String.valueOf(fragmentation));
            put(HTTP_SERVER_KEEP_ALIVE.getEnvVarName(), "false");
        }})) {
            serverMain.start();
            int httpPort = serverMain.getHttpServerPort();
            final String serverURL = "http://127.0.0.1:" + httpPort, username = "root", password = "root";

            OkHttpClient.Builder builder = new OkHttpClient
                    .Builder()
                    .addInterceptor(chain -> {
                        // This forces OkHttp to send data in chunks
                        // because the body length is unknown
                        Request originalRequest = chain.request();
                        RequestBody body = originalRequest.body();
                        Request newRequest = originalRequest.newBuilder()
                                .method(originalRequest.method(), chunkedBody(body)).build();
                        return chain.proceed(newRequest);
                    });

            int totalCount = 0;
            int requests = 1 + rnd.nextInt(4);
            for (int i = 0; i < requests; i++) {
                try (final InfluxDB influxDB = InfluxDBFactory.connect(serverURL, username, password, builder)) {
                    int lineCount = 100 + rnd.nextInt(2000);
                    influxDB.setDatabase("m1");
                    influxDB.write(createLines(lineCount));
                    totalCount += lineCount;
                }
            }

            serverMain.awaitTable("m1");

            assertSql(serverMain.getEngine(), "select count() from m1", "count()\n" +
                    totalCount + "\n");
        }
    }

    @Test
    public void testChunkedUpperCaseChunkSize() throws SqlException {
        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();

            Rnd rnd = generateRandom(LOG);

            int totalCount = 0;
            try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                String line = "line,sym1=123 field1=123i 1234567890000000000\n";

                for (int r = 0; r < 3; r++) {
                    int count = 1 + rnd.nextInt(100);
                    HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                    request.POST()
                            .url("/write ")
                            .withChunkedContent();

                    String hexChunkLen = Integer.toHexString(line.length() * count);
                    if (rnd.nextBoolean()) {
                        hexChunkLen = hexChunkLen.toUpperCase();
                    } else {
                        hexChunkLen = hexChunkLen.toLowerCase();
                    }
                    request.putAscii(hexChunkLen).putEOL();

                    for (int i = 0; i < count; i++) {
                        request.putAscii(line);
                    }

                    request.putEOL().putAscii("0").putEOL().putEOL();
                    try (HttpClient.ResponseHeaders resp = request.send(5000)) {
                        resp.await();
                        totalCount += count;
                    }
                }
            }

            serverMain.awaitTable("line");
            assertSql(serverMain.getEngine(), "select count() from line", "count()\n" +
                    totalCount + "\n");
        }
    }

    @Test
    public void testFuzzChunkedDataIlpUpload() throws SqlException {
        Rnd rnd = generateRandom(LOG);
        int fragmentation = 10 + rnd.nextInt(5);
        LOG.info().$("=== fragmentation=").$(fragmentation).$();

        try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
            put(DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), String.valueOf(fragmentation));
        }})) {
            serverMain.start();
            int httpPort = serverMain.getHttpServerPort();
            final String serverURL = "http://127.0.0.1:" + httpPort, username = "root", password = "root";

            OkHttpClient.Builder builder = new OkHttpClient
                    .Builder()
                    .addInterceptor(chain -> {
                        // This forces OkHttp to send data in chunks
                        // because the body length is unknown
                        Request originalRequest = chain.request();
                        RequestBody body = originalRequest.body();
                        Request newRequest = originalRequest.newBuilder()
                                .method(originalRequest.method(), chunkedBody(body)).build();
                        return chain.proceed(newRequest);
                    });

            int totalCount = 0;
            int requests = 1 + rnd.nextInt(4);
            for (int i = 0; i < requests; i++) {
                try (final InfluxDB influxDB = InfluxDBFactory.connect(serverURL, username, password, builder)) {
                    int lineCount = 100 + rnd.nextInt(2000);
                    influxDB.setDatabase("m1");
                    influxDB.write(createLines(lineCount));
                    totalCount += lineCount;
                }
            }

            serverMain.awaitTable("m1");
            assertSql(serverMain.getEngine(), "select count() from m1", "count()\n" +
                    totalCount + "\n");
        }
    }

    @Test
    public void testMultipartDataIlpUpload() throws Exception {
        try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
            put(DEBUG_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "5");
        }})) {
            serverMain.start();
            String lines = createLines(1024);
            RequestBody requestBody = new MultipartBody.Builder()
                    .addPart(MultipartBody.Part.createFormData("part1", lines))
                    .addPart(MultipartBody.Part.createFormData("part2", lines))
                    .build();
            Request request = newHttpRequest(serverMain, requestBody);

            OkHttpClient client = new OkHttpClient.Builder().build();
            try (Response response = client.newCall(request).execute()) {
                Assert.assertEquals(204, response.code());
            }

            RequestBody requestBody2 = new MultipartBody.Builder()
                    .setType(MultipartBody.FORM)
                    .addPart(MultipartBody.Part.createFormData("part1", lines))
                    .addPart(MultipartBody.Part.createFormData("part2", lines))
                    .build();
            Request request2 = newHttpRequest(serverMain, requestBody2);
            try (Response response = client.newCall(request2).execute()) {
                Assert.assertEquals(204, response.code());
            }

            serverMain.awaitTable("m1");
            assertSql(serverMain.getEngine(), "select count() from m1", "count()\n" +
                    "4096\n");
        }
    }

    @Test
    public void testMultipartFileIlpUpload() throws Exception {
        try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
            put(DEBUG_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "5");
        }})) {
            serverMain.start();
            String lines = createLines(1024);
            RequestBody requestBody = new MultipartBody.Builder()
                    .setType(MultipartBody.FORM)
                    .addPart(MultipartBody.Part.createFormData("part1", lines))
                    .addPart(MultipartBody.Part.createFormData("part2", lines))
                    .build();
            Request request = newHttpRequest(serverMain, requestBody);

            OkHttpClient client = new OkHttpClient.Builder().build();
            try (Response response = client.newCall(request).execute()) {
                Assert.assertEquals(204, response.code());
            }
            serverMain.awaitTable("m1");

            assertSql(serverMain.getEngine(), "select count() from m1", "count()\n" +
                    "2048\n");
        }
    }

    @Test
    public void testValidRequestAfterInvalidWithKeepAlive() throws Exception {
        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();
            Rnd rnd = generateRandom(LOG);

            int totalCount = 0;
            try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                String line = "line,sym1=123 field1=123i 1234567890000000000\n";

                for (int r = 0; r < 30; r++) {
                    if (r % 3 == 0) {
                        int count = 1 + rnd.nextInt(1000);
                        HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                        request.POST()
                                .url("/not_found_chunked ")
                                .withChunkedContent();
                        String hexChunkLen = Integer.toHexString(line.length() * count);
                        hexChunkLen = hexChunkLen.toUpperCase();
                        request.putAscii(hexChunkLen).putEOL();

                        for (int i = 0; i < count; i++) {
                            request.putAscii(line);
                        }
                        request.putEOL().putAscii("0").putEOL().putEOL();
                        try (HttpClient.ResponseHeaders resp = request.send(5000)) {
                            resp.await();
                        }
                    } else if (r % 3 == 1) {
                        // Good request
                        int count = 1 + rnd.nextInt(100);
                        HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                        request.POST()
                                .url("/write ")
                                .withChunkedContent();

                        String hexChunkLen = Integer.toHexString(line.length() * count);
                        hexChunkLen = hexChunkLen.toLowerCase();
                        request.putAscii(hexChunkLen).putEOL();

                        for (int i = 0; i < count; i++) {
                            request.putAscii(line);
                        }

                        request.putEOL().putAscii("0").putEOL().putEOL();
                        try (HttpClient.ResponseHeaders resp = request.send(5000)) {
                            resp.await();
                            totalCount += count;
                        }
                    } else {
                        // Good request
                        int count = 1 + rnd.nextInt(100);
                        HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                        request.POST()
                                .url("/not_found ")
                                .withContent();

                        for (int i = 0; i < count; i++) {
                            request.putAscii(line);
                        }

                        request.putEOL().putAscii("0").putEOL().putEOL();
                        try (HttpClient.ResponseHeaders resp = request.send(5000)) {
                            resp.await();
                        }
                    }
                }
            }

            serverMain.awaitTable("line");
            assertSql(serverMain.getEngine(), "select count() from line", "count()\n" +
                    totalCount + "\n");
        }
    }

    private RequestBody chunkedBody(final RequestBody body) {
        return new RequestBody() {
            @Override
            public long contentLength() {
                // Forces OkHttp to send data in chunks
                return -1;
            }

            @Nullable
            @Override
            public MediaType contentType() {
                return body.contentType();
            }

            @Override
            public void writeTo(@NotNull BufferedSink bufferedSink) throws IOException {
                body.writeTo(bufferedSink);
            }
        };
    }

    private String createLines(int lineCount) {
        StringSink sink = new StringSink();
        for (int i = 0; i < lineCount; i++) {
            sink.put("m1,t1=1 f1=" + i + "i,f2=1.1,f3=\"abc\" 1000000\n");
        }
        return sink.toString();
    }

    static Request newHttpRequest(ServerMain serverMain, RequestBody requestBody) {
        int port = serverMain.getHttpServerPort();
        HttpUrl httpUrl = new HttpUrl.Builder()
                .scheme("http")
                .host("127.0.0.1")
                .port(port)
                .addPathSegment("write")
                .build();
        final Request.Builder request = new Request.Builder();
        return request
                .url(httpUrl)
                .post(requestBody)
                .build();
    }
}
