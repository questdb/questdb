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
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class SqlValidationTest extends AbstractCairoTest {

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
                            try (TestHttpClient testHttpClient = new TestHttpClient()) {
                                testHttpClient.setKeepConnection(true);
                                int iterCount = rnd.nextInt(10);
                                for (int i = 0; i < iterCount; i++) {
                                    int index = rnd.nextInt(candidateCount);
                                    String expectedResponse = requestResponse[index][1].toString();
                                    HttpClient.Request req = testHttpClient.getHttpClient().newRequest("127.0.0.1", 9001);
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
    public void testDoesNotMessUpTable() throws Exception {
        execute("create table xyz as (select rnd_int() a from long_sequence(1000))");
        Rnd rnd = TestUtils.generateRandom(LOG);
        getSimpleTester()
                .withForceRecvFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withForceSendFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .run((HttpQueryTestBuilder.HttpClientCode) (engine, sqlExecutionContext) -> {
                    try (TestHttpClient testHttpClient = new TestHttpClient()) {
                        testHttpClient.assertGet(
                                "/api/v1/sql/validate",
                                "{\"queryType\":\"DROP\"}",
                                "drop table xyz"
                        );

                        testHttpClient.assertGet(
                                "/api/v1/sql/validate",
                                "{\"queryType\":\"RENAME TABLE\"}",
                                "rename table xyz to abc"
                        );

                        testHttpClient.assertGet(
                                "/api/v1/sql/validate",
                                "{\"queryType\":\"TRUNCATE\"}",
                                "truncate table xyz"
                        );

                        testHttpClient.assertGet(
                                "/api/v1/sql/validate",
                                "{\"queryType\":\"ALTER TABLE\"}",
                                "alter table xyz rename column a to b"
                        );

                        // cancel query should not error out, it doesn't cancel anything actually
                        testHttpClient.assertGet(
                                "/api/v1/sql/validate",
                                "{\"queryType\":\"CANCEL QUERY\"}",
                                "cancel query 1111"
                        );

                        testHttpClient.assertGet(
                                "/api/v1/sql/validate",
                                "{\"queryType\":\"CHECKPOINT RELEASE\"}",
                                "checkpoint release"
                        );

                        testHttpClient.assertGet(
                                "/api/v1/sql/validate",
                                "{\"queryType\":\"CHECKPOINT CREATE\"}",
                                "checkpoint create"
                        );

                        // make sure checkpoint is not in progress
                        testHttpClient.assertGet(
                                "/exec",
                                "{\"query\":\"SELECT * FROM checkpoint_status();\",\"columns\":[{\"name\":\"in_progress\",\"type\":\"BOOLEAN\"},{\"name\":\"started_at\",\"type\":\"TIMESTAMP\"}],\"timestamp\":-1,\"dataset\":[[false,null]],\"count\":1}",
                                "SELECT * FROM checkpoint_status();"
                        );

                        // we should not be able to create mat view
                        testHttpClient.assertGet(
                                "/api/v1/sql/validate",
                                "{\"queryType\":\"CREATE MAT VIEW\"}",
                                """
                                        CREATE MATERIALIZED VIEW 'trades_OHLC_15m'
                                        WITH BASE 'trades' REFRESH IMMEDIATE AS
                                                SELECT
                                        timestamp, symbol,
                                                first(price) AS open,
                                        max(price) as high,
                                        min(price) as low,
                                        last(price) AS close,
                                        sum(amount) AS volume
                                        FROM trades
                                        SAMPLE BY 15m;
                                        """
                        );

                        // check that mat view does not exist
                        TestUtils.assertException(
                                engine,
                                sqlExecutionContext,
                                "select * from trades_OHLC_15m",
                                "table does not exist [table=trades_OHLC_15m]",
                                14,
                                sink
                        );

                        testHttpClient.assertGet(
                                "/api/v1/sql/validate",
                                "{\"query\":\"backup table hello\",\"error\":\"backup is disabled, server.conf property 'cairo.sql.backup.root' is not set\",\"position\":0}",
                                "backup table hello"
                        );

                        testHttpClient.assertGet(
                                "/api/v1/sql/validate",
                                "{\"query\":\"backup database\",\"error\":\"backup is disabled, server.conf property 'cairo.sql.backup.root' is not set\",\"position\":0}",
                                "backup database"
                        );

                        testHttpClient.assertGet(
                                "/api/v1/sql/validate",
                                "{\"queryType\":\"CREATE TABLE\"}",
                                "create table should_not_exist(a int, ts timestamp) timestamp(ts) partition by hour"
                        );

                        // check that table is still exists
                        TestUtils.assertSql(
                                engine,
                                sqlExecutionContext,
                                "select count(a) from xyz",
                                sink,
                                """
                                        count
                                        1000
                                        """
                        );

                        TestUtils.assertException(
                                engine,
                                sqlExecutionContext,
                                "select * from should_not_exist",
                                "table does not exist [table=should_not_exist]",
                                14,
                                sink
                        );
                    }
                });
    }

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
                                                requestResponse[index][0].toString()
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
                                        "select * from xyz limit 10"
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
                                "select count() from xyz"
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
                                        "select a, b from xyz"
                                );
                            }
                        }
                );
    }
}
