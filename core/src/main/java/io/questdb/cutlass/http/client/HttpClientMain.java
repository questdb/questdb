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

package io.questdb.cutlass.http.client;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cutlass.http.client.ser.JsonToTableSerializer;
import io.questdb.cutlass.json.JsonException;

public class HttpClientMain {

    public static void main(String[] args) throws JsonException {
        DefaultCairoConfiguration configuration = new DefaultCairoConfiguration("C:\\qdb2\\db");
        try (
                CairoEngine engine = new CairoEngine(configuration);
                JsonToTableSerializer jsonToTableSerializer = new JsonToTableSerializer(engine);
                HttpClient client = HttpClientFactory.newPlainTextInstance()
        ) {
            for (int i = 0; i < 1; i++) {
                HttpClient.Request req = client.newRequest("localhost", 9000);
                try (
                        HttpClient.ResponseHeaders responseHeaders = req
                                .GET()
                                .url("/exec")
                                .query("query", "cpu limit 2")
                                //.query("query", "cpu")
                                .header("Accept", "gzip, deflate, br")
                                .header("SomethingElse", "vlad")
                                .authBasic("vlad", "hello")
                                .send()
                ) {
                    responseHeaders.await();

                    jsonToTableSerializer.clear();

                    Response response = responseHeaders.getResponse();
                    Fragment fragment;

                    long t = System.currentTimeMillis();
                    int chunkCount = 0;
                    while ((fragment = response.recv()) != null) {
                        jsonToTableSerializer.parse(fragment.lo(), fragment.hi());
                        chunkCount++;
                    }

                    System.out.println(System.currentTimeMillis() - t);
                    System.out.println("done: " + i + ", chunks: " + chunkCount);
                }
            }
        }
    }
}
