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
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class SqlValidationTest extends AbstractCairoTest {

    @Test
    public void testFullFuzz() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        getSimpleTester()
                .withForceRecvFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withForceSendFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                // send buffer has to be large enough for the error message and the http header (maybe we should truncate the message if it doesn't fit?)
                .withSendBufferSize(Math.max(1024, rnd.nextInt(4099)))
                .run((HttpQueryTestBuilder.HttpClientCode) (engine, sqlExecutionContext) -> {
                            engine.execute("create table xyz as (select rnd_int() a, rnd_double() b, timestamp_sequence(0,1000) ts from long_sequence(1000)) timestamp(ts) partition by hour");

                            var requestResponse = new Object[][]{
                                    {"select count() from xyz", "{\"query\":\"select count() from xyz\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"timestamp\":-1}"},
                                    {"select a from xyz limit 1", "{\"query\":\"select a from xyz limit 1\",\"columns\":[{\"name\":\"a\",\"type\":\"INT\"}],\"timestamp\":-1}"},
                                    {"select b from xyz limit 5", "{\"query\":\"select b from xyz limit 5\",\"columns\":[{\"name\":\"b\",\"type\":\"DOUBLE\"}],\"timestamp\":-1}"},
                                    {"select ts, b from xyz limit 15", "{\"query\":\"select ts, b from xyz limit 15\",\"columns\":[{\"name\":\"ts\",\"type\":\"TIMESTAMP\"},{\"name\":\"b\",\"type\":\"DOUBLE\"}],\"timestamp\":0}"},
                                    {"select a, z from xyz", "{\"query\":\"select a, z from xyz\",\"error\":\"Invalid column: z\",\"position\":10}"},
                                    {"create table abc(x int)", "{\"queryType\":\"CREATE TABLE\"}"},
                                    {"select \"µ\" from xyz", "{\"query\":\"select \\\"µ\\\" from xyz\",\"error\":\"Invalid column: µ\",\"position\":7}"},
                                    {new Utf8StringSink().put("select").putAny((byte) 0xC3).putAny((byte) 0x28), "{\"query\":\"selectￃ(\",\"error\":\"Bad UTF8 encoding in query text\",\"position\":0}"},
                                    // empty query
                                    {"", "{\"error\":\"empty query\",\"query\":\"\",\"position\":\"0\"}"}
                            };

                            var candidateCount = requestResponse.length;
                            try (TestHttpClient testHttpClient = new TestHttpClient()) {
                                testHttpClient.setKeepConnection(true);
                                int iterCount = rnd.nextInt(10);
                                for (int i = 0; i < iterCount; i++) {
                                    int index = rnd.nextInt(candidateCount);
                                    if (requestResponse[index][0] instanceof Utf8Sequence utf8Sql) {
                                        testHttpClient.assertGet(
                                                "/api/v1/sql/validate",
                                                requestResponse[index][1].toString(),
                                                utf8Sql
                                        );
                                    } else {
                                        testHttpClient.assertGet(
                                                "/api/v1/sql/validate",
                                                requestResponse[index][1].toString(),
                                                requestResponse[index][0].toString(),
                                                "localhost",
                                                9001,
                                                null,
                                                null,
                                                null
                                        );
                                    }
                                }
                            }
                        }
                );
    }

    @Test
    public void testValidationAllColumnTypes() throws Exception {
        execute("""
                create table xyz as (
                  select
                    rnd_boolean() col_boolean,
                    rnd_byte(0, 127) col_byte,
                    rnd_short(0, 1000) col_short,
                    rnd_int(0, 1000000, 0) col_int,
                    rnd_long(0, 1000000, 0) col_long,
                    rnd_float(0) col_float,
                    rnd_double(0) col_double,
                    rnd_long256() col_long256,
                    rnd_varchar(1, 40, 1) col_varchar,
                    rnd_str(5, 20, 1) col_string,
                    rnd_char() col_char,
                    rnd_symbol('AAPL', 'GOOGL', 'MSFT', 'AMZN') col_symbol,
                    rnd_date(to_date('2020', 'yyyy'), to_date('2024', 'yyyy'), 0) col_date,
                    rnd_timestamp(to_timestamp('2020', 'yyyy'), to_timestamp('2024', 'yyyy'), 0) col_timestamp,
                    rnd_bin(10, 100, 1) col_binary,
                    rnd_ipv4() col_ipv4,
                    rnd_uuid4() col_uuid,
                    rnd_geohash(8) col_geohash_byte,
                    rnd_geohash(16) col_geohash_short,
                    rnd_geohash(32) col_geohash_int,
                    array[rnd_double(), rnd_double(), rnd_double()] col_array
                  from long_sequence(1000)
                ) timestamp(col_timestamp) partition by month
                """
        );
        Rnd rnd = TestUtils.generateRandom(LOG);
        getSimpleTester()
                .withForceRecvFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withForceSendFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .run((HttpQueryTestBuilder.HttpClientCode) (engine, sqlExecutionContext) -> {
                            try (TestHttpClient testHttpClient = new TestHttpClient()) {
                                testHttpClient.assertGet(
                                        "/api/v1/sql/validate",
                                        "{\"query\":\"select * from xyz limit 10\"," +
                                                "\"columns\":[" +
                                                "{\"name\":\"col_boolean\",\"type\":\"BOOLEAN\"}," +
                                                "{\"name\":\"col_byte\",\"type\":\"BYTE\"}," +
                                                "{\"name\":\"col_short\",\"type\":\"SHORT\"}," +
                                                "{\"name\":\"col_int\",\"type\":\"INT\"}," +
                                                "{\"name\":\"col_long\",\"type\":\"LONG\"}," +
                                                "{\"name\":\"col_float\",\"type\":\"FLOAT\"}," +
                                                "{\"name\":\"col_double\",\"type\":\"DOUBLE\"}," +
                                                "{\"name\":\"col_long256\",\"type\":\"LONG256\"}," +
                                                "{\"name\":\"col_varchar\",\"type\":\"VARCHAR\"}," +
                                                "{\"name\":\"col_string\",\"type\":\"STRING\"}," +
                                                "{\"name\":\"col_char\",\"type\":\"CHAR\"}," +
                                                "{\"name\":\"col_symbol\",\"type\":\"SYMBOL\"}," +
                                                "{\"name\":\"col_date\",\"type\":\"DATE\"}," +
                                                "{\"name\":\"col_timestamp\",\"type\":\"TIMESTAMP\"}," +
                                                "{\"name\":\"col_binary\",\"type\":\"BINARY\"}," +
                                                "{\"name\":\"col_ipv4\",\"type\":\"IPv4\"}," +
                                                "{\"name\":\"col_uuid\",\"type\":\"UUID\"}," +
                                                "{\"name\":\"col_geohash_byte\",\"type\":\"GEOHASH(8b)\"}," +
                                                "{\"name\":\"col_geohash_short\",\"type\":\"GEOHASH(16b)\"}," +
                                                "{\"name\":\"col_geohash_int\",\"type\":\"GEOHASH(32b)\"}," +
                                                "{\"name\":\"col_array\",\"type\":\"ARRAY\",\"dim\":1,\"elemType\":\"DOUBLE\"}" +
                                                "],\"timestamp\":13}",
                                        "select * from xyz limit 10",
                                        "localhost",
                                        9001,
                                        null,
                                        null,
                                        null
                                );
                            }
                        }
                );
    }

    @Test
    public void testValidationOk() throws Exception {
        execute("create table xyz as (select rnd_int() a from long_sequence(1000))");
        Rnd rnd = TestUtils.generateRandom(LOG);
        getSimpleTester()
                .withForceRecvFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withForceSendFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .run((HttpQueryTestBuilder.HttpClientCode) (engine, sqlExecutionContext) -> {
                    try (TestHttpClient testHttpClient = new TestHttpClient()) {
                        testHttpClient.assertGet(
                                "/api/v1/sql/validate",
                                "{\"query\":\"select count() from xyz\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"timestamp\":-1}",
                                "select count() from xyz",
                                "localhost",
                                9001,
                                null,
                                null,
                                null
                        );
                    }
                });
    }

    @Test
    public void testValidationSyntaxError() throws Exception {
        execute("create table xyz as (select rnd_int() a from long_sequence(1000))");
        Rnd rnd = TestUtils.generateRandom(LOG);
        getSimpleTester()
                .withForceRecvFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withForceSendFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .run((HttpQueryTestBuilder.HttpClientCode) (engine, sqlExecutionContext) -> {
                            try (TestHttpClient testHttpClient = new TestHttpClient()) {
                                testHttpClient.assertGet(
                                        "/api/v1/sql/validate",
                                        "{\"query\":\"select a, b from xyz\",\"error\":\"Invalid column: b\",\"position\":10}",
                                        "select a, b from xyz",
                                        "localhost",
                                        9001,
                                        null,
                                        null,
                                        null
                                );
                            }
                        }
                );
    }
}