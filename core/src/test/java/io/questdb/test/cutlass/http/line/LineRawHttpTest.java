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

package io.questdb.test.cutlass.http.line;

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.ServerMain;
import io.questdb.cairo.TableToken;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import okhttp3.*;
import okio.BufferedSink;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.questdb.PropertyKey.*;
import static io.questdb.test.cutlass.http.line.IlpHttpUtils.getHttpPort;

public class LineRawHttpTest extends AbstractBootstrapTest {
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testChunkedDataIlpUploadNoKeepAlive() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            int fragmentation = 1 + rnd.nextInt(5);
            LOG.info().$("=== fragmentation=").$(fragmentation).$();
            try (final TestServerMain serverMain = startWithEnvVariables(
                    DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), String.valueOf(fragmentation),
                    HTTP_SERVER_KEEP_ALIVE.getEnvVarName(), "false"
            )) {
                int httpPort = getHttpPort(serverMain);
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

                serverMain.waitWalTxnApplied("m1");

                serverMain.assertSql("select count() from m1", "count\n" +
                        totalCount + "\n");
            }
        });
    }

    @Test
    public void testChunkedUpperCaseChunkSize() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
            )) {
                serverMain.start();

                Rnd rnd = TestUtils.generateRandom(LOG);

                int totalCount = 0;
                try (HttpClient httpClient = HttpClientFactory.newInstance(new DefaultHttpClientConfiguration())) {
                    String line = "line,sym1=123 field1=123i 1234567890000000000\n";

                    for (int r = 0; r < 3; r++) {
                        int count = 1 + rnd.nextInt(100);
                        HttpClient.Request request = httpClient.newRequest();
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
                        HttpClient.ResponseHeaders resp = request.send("localhost", getHttpPort(serverMain), 5000);
                        resp.await();
                        totalCount += count;
                    }
                }

                serverMain.waitWalTxnApplied("line");
                serverMain.assertSql("select count() from line", "count\n" +
                        totalCount + "\n");
            }
        });
    }

    @Test
    public void testFuzzChunkedDataIlpUpload() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            int fragmentation = 1 + rnd.nextInt(5);
            LOG.info().$("=== fragmentation=").$(fragmentation).$();
            try (final TestServerMain serverMain = startWithEnvVariables(
                    DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), String.valueOf(fragmentation)
            )) {
                int httpPort = getHttpPort(serverMain);
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

                serverMain.waitWalTxnApplied("m1");

                serverMain.assertSql("select count() from m1", "count\n" +
                        totalCount + "\n");
            }
        });
    }

    @Test
    public void testMultipartDataIlpUpload() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    DEBUG_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "5"
            )) {
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

                serverMain.waitWalTxnApplied("m1");

                serverMain.assertSql("select count() from m1", "count\n" +
                        "4096\n");
            }
        });
    }

    @Test
    public void testMultipartFileIlpUpload() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    DEBUG_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "5"
            )) {
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
                serverMain.waitWalTxnApplied("m1");

                serverMain.assertSql("select count() from m1", "count\n" +
                        "2048\n");
            }
        });
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

    static Request newHttpRequest(TestServerMain serverMain, RequestBody requestBody) {
        int port = getHttpPort(serverMain);
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
