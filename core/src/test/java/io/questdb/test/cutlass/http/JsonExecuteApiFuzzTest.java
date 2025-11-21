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

import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class JsonExecuteApiFuzzTest extends AbstractCairoTest {

    @Test
    public void testFullFuzz() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);


        var emptyColList = new CharSequenceObjHashMap<>();
        emptyColList.put("cols", ",");

        var nonExistingColList = new CharSequenceObjHashMap<>();
        nonExistingColList.put("cols", "z");

        getSimpleTester()
                .withForceRecvFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withForceSendFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withSendBufferSize(Math.max(1024, rnd.nextInt(4099)))
                .run((HttpQueryTestBuilder.HttpClientCode) (engine, sqlExecutionContext) -> {
                            engine.execute("create table xyz as (select rnd_int() a, rnd_double() b, timestamp_sequence(0,1000) ts from long_sequence(1000)) timestamp(ts) partition by hour");
                            engine.execute("create table growing as (select rnd_int() a, rnd_double() b, timestamp_sequence(0,1000) ts from long_sequence(1000)) timestamp(ts) partition by hour");

                            var requestResponse = new Object[][]{
                                    {"select count() from xyz", "{\"query\":\"select count() from xyz\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[1000]],\"count\":1}"},
                                    {"select a from xyz limit 1", "{\"query\":\"select a from xyz limit 1\",\"columns\":[{\"name\":\"a\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[-1148479920]],\"count\":1}"},
                                    {"select a from xyz limit 1", "{\"query\":\"select a from xyz limit 1\",\"error\":\"empty column in query parameter\"}", emptyColList},
                                    {"select a from xyz limit 1", "{\"query\":\"select a from xyz limit 1\",\"error\":\"column not found: 'z'\"}", nonExistingColList},
                                    {"select b from xyz limit 5", "{\"query\":\"select b from xyz limit 5\",\"columns\":[{\"name\":\"b\",\"type\":\"DOUBLE\"}],\"timestamp\":-1,\"dataset\":[[0.8043224099968393],[0.08486964232560668],[0.0843832076262595],[0.6508594025855301],[0.7905675319675964]],\"count\":5}"},
                                    {"select ts, b from xyz limit 15", "{\"query\":\"select ts, b from xyz limit 15\",\"columns\":[{\"name\":\"ts\",\"type\":\"TIMESTAMP\"},{\"name\":\"b\",\"type\":\"DOUBLE\"}],\"timestamp\":0,\"dataset\":[[\"1970-01-01T00:00:00.000000Z\",0.8043224099968393],[\"1970-01-01T00:00:00.001000Z\",0.08486964232560668],[\"1970-01-01T00:00:00.002000Z\",0.0843832076262595],[\"1970-01-01T00:00:00.003000Z\",0.6508594025855301],[\"1970-01-01T00:00:00.004000Z\",0.7905675319675964],[\"1970-01-01T00:00:00.005000Z\",0.22452340856088226],[\"1970-01-01T00:00:00.006000Z\",0.3491070363730514],[\"1970-01-01T00:00:00.007000Z\",0.7611029514995744],[\"1970-01-01T00:00:00.008000Z\",0.4217768841969397],[\"1970-01-01T00:00:00.009000Z\",0.0367581207471136],[\"1970-01-01T00:00:00.010000Z\",0.6276954028373309],[\"1970-01-01T00:00:00.011000Z\",0.6778564558839208],[\"1970-01-01T00:00:00.012000Z\",0.8756771741121929],[\"1970-01-01T00:00:00.013000Z\",0.8799634725391621],[\"1970-01-01T00:00:00.014000Z\",0.5249321062686694]],\"count\":15}"},
                                    {"select a, z from xyz", "{\"query\":\"select a, z from xyz\",\"error\":\"Invalid column: z\",\"position\":10}"},
                                    {"create table if not exists abc(x int)", "{\"ddl\":\"OK\"}"},
                                    {"select \"µ\" from xyz", "{\"query\":\"select \\\"µ\\\" from xyz\",\"error\":\"Invalid column: µ\",\"position\":7}"},
                                    {new Utf8StringSink().put("select").putAny((byte) 0xC3).putAny((byte) 0x28), "{\"query\":\"selectￃ(\",\"error\":\"Bad UTF8 encoding in query text\",\"position\":0}"},
                                    // empty query
                                    {"", "{\"error\":\"empty query\",\"query\":\"\",\"position\":\"0\"}"},
                                    {"backup table xyz", "{\"query\":\"backup table xyz\",\"error\":\"backup is disabled, server.conf property 'cairo.sql.backup.root' is not set\",\"position\":0}"},
                                    {"insert into growing values (0, 0, '2000')", "{\"dml\":\"OK\"}"},
                                    // non-empty query text with an effectively empty query
                                    {"--", "{\"error\":\"empty query\",\"query\":\"--\",\"position\":\"0\"}"},
                                    {"update growing set a = 42 where a != a", "{\"dml\":\"OK\",\"updated\":0}"}
                            };


                            var candidateCount = requestResponse.length;
                            try (TestHttpClient testHttpClient = new TestHttpClient()) {
                                testHttpClient.setKeepConnection(true);
                                int iterCount = rnd.nextInt(100);
                                for (int i = 0; i < iterCount; i++) {
                                    int index = rnd.nextInt(candidateCount);
                                    Object[] testCase = requestResponse[index];

                                    CharSequenceObjHashMap<String> queryParams = null;
                                    if (testCase.length > 2) {
                                        queryParams = (CharSequenceObjHashMap<String>) testCase[2];
                                    }


                                    if (testCase[0] instanceof Utf8Sequence utf8Sql) {
                                        testHttpClient.assertGet(
                                                "/api/v1/sql/execute",
                                                testCase[1].toString(),
                                                utf8Sql,
                                                queryParams
                                        );
                                    } else {
                                        testHttpClient.assertGet(
                                                "/api/v1/sql/execute",
                                                testCase[1].toString(),
                                                testCase[0].toString(),
                                                queryParams
                                        );
                                    }
                                }
                            }
                        }
                );
    }
}
