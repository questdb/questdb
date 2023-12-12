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

import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import okhttp3.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import static io.questdb.PropertyKey.DEBUG_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE;
import static io.questdb.test.cutlass.http.line.IlpHttpUtils.getHttpPort;

public class LineRawHttpTest extends AbstractBootstrapTest {
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
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
