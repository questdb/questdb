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

import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Ignore;
import org.junit.Test;

public class JsonExecuteApiFuzzTest extends AbstractCairoTest {

    @Test
    public void testFullFuzz() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        getSimpleTester()
                .withForceRecvFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withForceSendFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .run((HttpQueryTestBuilder.HttpClientCode) (engine, sqlExecutionContext) -> {
                            engine.execute("create table xyz as (select rnd_int() a, rnd_double() b, timestamp_sequence(0,1000) ts from long_sequence(1000)) timestamp(ts) partition by hour");

                            var requestResponse = new String[][]{
                                    {"select count() from xyz", "{\"query\":\"select count() from xyz\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[1000]],\"count\":1}"},
                                    {"select a from xyz limit 1", "{\"query\":\"select a from xyz limit 1\",\"columns\":[{\"name\":\"a\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[-1148479920]],\"count\":1}"},
                                    {"select b from xyz limit 5", "{\"query\":\"select b from xyz limit 5\",\"columns\":[{\"name\":\"b\",\"type\":\"DOUBLE\"}],\"timestamp\":-1,\"dataset\":[[0.8043224099968393],[0.08486964232560668],[0.0843832076262595],[0.6508594025855301],[0.7905675319675964]],\"count\":5}"},
                                    {"select ts, b from xyz limit 15", "{\"query\":\"select ts, b from xyz limit 15\",\"columns\":[{\"name\":\"ts\",\"type\":\"TIMESTAMP\"},{\"name\":\"b\",\"type\":\"DOUBLE\"}],\"timestamp\":0,\"dataset\":[[\"1970-01-01T00:00:00.000000Z\",0.8043224099968393],[\"1970-01-01T00:00:00.001000Z\",0.08486964232560668],[\"1970-01-01T00:00:00.002000Z\",0.0843832076262595],[\"1970-01-01T00:00:00.003000Z\",0.6508594025855301],[\"1970-01-01T00:00:00.004000Z\",0.7905675319675964],[\"1970-01-01T00:00:00.005000Z\",0.22452340856088226],[\"1970-01-01T00:00:00.006000Z\",0.3491070363730514],[\"1970-01-01T00:00:00.007000Z\",0.7611029514995744],[\"1970-01-01T00:00:00.008000Z\",0.4217768841969397],[\"1970-01-01T00:00:00.009000Z\",0.0367581207471136],[\"1970-01-01T00:00:00.010000Z\",0.6276954028373309],[\"1970-01-01T00:00:00.011000Z\",0.6778564558839208],[\"1970-01-01T00:00:00.012000Z\",0.8756771741121929],[\"1970-01-01T00:00:00.013000Z\",0.8799634725391621],[\"1970-01-01T00:00:00.014000Z\",0.5249321062686694]],\"count\":15}"},
                                    {"select a, z from xyz", "{\"query\":\"select a, z from xyz\",\"error\":\"Invalid column: z\",\"position\":10}"}
                            };

                            var candidateCount = requestResponse.length;
                            try (TestHttpClient testHttpClient = new TestHttpClient()) {
                                testHttpClient.setKeepConnection(true);
                                int iterCount = rnd.nextInt(10);
                                for (int i = 0; i < iterCount; i++) {
                                    int index = rnd.nextInt(candidateCount);
                                    testHttpClient.assertGet(
                                            "/api/v1/sql/execute",
                                            requestResponse[index][1],
                                            requestResponse[index][0],
                                            "localhost",
                                            9001,
                                            null,
                                            null,
                                            null
                                    );
                                }
                            }
                        }
                );
    }

}
