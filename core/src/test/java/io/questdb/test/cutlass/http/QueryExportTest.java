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

package io.questdb.test.cutlass.http;

import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class QueryExportTest extends AbstractCairoTest {

    @Test
    public void testCsvExportFuzz() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        getSimpleTester()
                .withForceRecvFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withForceSendFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withSendBufferSize(Math.max(1024, rnd.nextInt(4099)))
                .run((HttpQueryTestBuilder.HttpClientCode) (engine, sqlExecutionContext) -> {
                            engine.execute("""
                                    create table xyz as (select
                                        rnd_int() a,
                                        rnd_double() b,
                                        timestamp_sequence(0,1000) ts
                                        from long_sequence(1000)
                                    ) timestamp(ts) partition by hour""");

                            var requestResponse = new Object[][]{
                                    {"select count() from xyz", """
                                        "count"\r
                                        1000\r
                                        """},
                                    {"select a from xyz limit 1", """
                                        "a"\r
                                        -1148479920\r
                                        """},
                                    {"select b from xyz limit 5", """
                                         "b"\r
                                         0.8043224099968393\r
                                         0.08486964232560668\r
                                         0.0843832076262595\r
                                         0.6508594025855301\r
                                         0.7905675319675964\r
                                         """},
                                    {"select ts, b from xyz limit 15", """
                                        "ts","b"\r
                                        "1970-01-01T00:00:00.000000Z",0.8043224099968393\r
                                        "1970-01-01T00:00:00.001000Z",0.08486964232560668\r
                                        "1970-01-01T00:00:00.002000Z",0.0843832076262595\r
                                        "1970-01-01T00:00:00.003000Z",0.6508594025855301\r
                                        "1970-01-01T00:00:00.004000Z",0.7905675319675964\r
                                        "1970-01-01T00:00:00.005000Z",0.22452340856088226\r
                                        "1970-01-01T00:00:00.006000Z",0.3491070363730514\r
                                        "1970-01-01T00:00:00.007000Z",0.7611029514995744\r
                                        "1970-01-01T00:00:00.008000Z",0.4217768841969397\r
                                        "1970-01-01T00:00:00.009000Z",0.0367581207471136\r
                                        "1970-01-01T00:00:00.010000Z",0.6276954028373309\r
                                        "1970-01-01T00:00:00.011000Z",0.6778564558839208\r
                                        "1970-01-01T00:00:00.012000Z",0.8756771741121929\r
                                        "1970-01-01T00:00:00.013000Z",0.8799634725391621\r
                                        "1970-01-01T00:00:00.014000Z",0.5249321062686694\r
                                        """},
                                    {"select a, z from xyz", """
                                        {"query":"select a, z from xyz","error":"Invalid column: z","position":10}"""},
                                    {"create table abc(x int)", """
                                        {"query":"create table abc(x int)","error":"/exp endpoint only accepts SELECT","position":0}"""},
                                    {"select \"µ\" from xyz", """
                                        {"query":"select \\"µ\\" from xyz","error":"Invalid column: µ","position":7}"""},
                                    {new Utf8StringSink().put("select").putAny((byte) 0xC3).putAny((byte) 0x28), """
                                        {"query":"select","error":"Bad UTF8 encoding in query text","position":0}"""},
                                    {"", """
                                        {"query":"","error":"No query text","position":0}"""}
                            };

                            var candidateCount = requestResponse.length;
                            String[] hostnames = {"localhost", "127.0.0.1"};
                            int hostnameIndex = 0;
                            try (TestHttpClient testHttpClient = new TestHttpClient()) {
                                testHttpClient.setKeepConnection(true);
                                for (int i = 0; i < 100; i++) {
                                    int index = rnd.nextInt(candidateCount);
                                    if (rnd.nextInt(100) < 5) {
                                        hostnameIndex = 1 - hostnameIndex;
                                    }
                                    String expectedResponse = requestResponse[index][1].toString();
                                    HttpClient.Request req = testHttpClient.getHttpClient().newRequest(hostnames[hostnameIndex], 9001);
                                    req.GET().url("/exp");
                                    if (requestResponse[index][0] instanceof Utf8Sequence sql) {
                                        req.query("query", sql);
                                    } else {
                                        req.query("query", requestResponse[index][0].toString());
                                    }
                                    req.query("fmt", "csv");
                                    testHttpClient.reqToSink(req, testHttpClient.sink, null, null, null, null);
                                    TestUtils.assertEquals(expectedResponse, testHttpClient.sink);
                                }
                            }
                        }
                );
    }

    @Test
    public void testExportParquetFuzzDisabled() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        getSimpleTester()
                .withForceRecvFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withForceSendFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                // send buffer has to be large enough for the error message and the http header (maybe we should truncate the message if it doesn't fit?)
                .withSendBufferSize(Math.max(1024, rnd.nextInt(4099)))
                .run((HttpQueryTestBuilder.HttpClientCode) (engine, sqlExecutionContext) -> {
                            engine.execute("""
                                    create table xyz as (select
                                        rnd_int() a,
                                        rnd_double() b,
                                        timestamp_sequence(0,1000) ts
                                        from long_sequence(1000)
                                    ) timestamp(ts) partition by hour""");

                            var requestResponse = new Object[][]{
                                    {"create table abc (ts TIMESTAMP)", """
                                        {"query":"create table abc (ts TIMESTAMP)","error":"/exp endpoint only accepts SELECT","position":0}"""}
                            };

                            var candidateCount = requestResponse.length;
                            try (TestHttpClient testHttpClient = new TestHttpClient()) {
                                testHttpClient.setKeepConnection(true);
                                for (int i = 0; i < 100; i++) {
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
