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

import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class ParquetExportTest extends AbstractCairoTest {

    @Test
    public void testFullFuzzDisabledState() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        getSimpleTester()
                .withForceRecvFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withForceSendFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withSendBufferSize(Math.max(1024, rnd.nextInt(4096)))
                // send buffer has to be large enough for the error message and the http header (maybe we should truncate the message if it doesn't fit?)
                .withSendBufferSize(Math.max(1024, rnd.nextInt(4099)))
                .run((HttpQueryTestBuilder.HttpClientCode) (engine, sqlExecutionContext) -> {
                            engine.execute("create table xyz as (select rnd_int() a, rnd_double() b, timestamp_sequence(0,1000) ts from long_sequence(1000)) timestamp(ts) partition by hour");

                            var requestResponse = new Object[][]{
                                    {"select count() from xyz", "{\"query\":\"select count() from xyz\",\"error\":\"parquet export is disabled ['cairo.sql.copy.export.root' is not set]\",\"position\":0}"},
                                    {"select * from xyz", "{\"query\":\"select * from xyz\",\"error\":\"parquet export is disabled ['cairo.sql.copy.export.root' is not set]\",\"position\":0}"},
                            };

                            var candidateCount = requestResponse.length;
                            try (TestHttpClient testHttpClient = new TestHttpClient()) {
                                testHttpClient.setKeepConnection(true);
                                int iterCount = rnd.nextInt(10);
                                for (int i = 0; i < iterCount; i++) {
                                    System.out.println("iteration: " + i);
                                    int index = rnd.nextInt(candidateCount);
                                    CharSequence expectedResponse = requestResponse[index][1].toString();
                                    HttpClient.Request req = testHttpClient.getHttpClient().newRequest("127.0.0.1", 9001);
                                    req.GET().url("/exp");

                                    if (requestResponse[index][0] instanceof Utf8Sequence sql) {
                                        req.query("query", sql);
                                    } else {
                                        req.query("query", requestResponse[index][0].toString());
                                    }

                                    req.query("fmt", "parquet");
                                    testHttpClient.reqToSink(req, testHttpClient.sink, null, null, null, null);
                                    TestUtils.assertEquals(expectedResponse, testHttpClient.sink);
                                }
                            }
                        }
                );
    }

}