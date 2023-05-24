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

package io.questdb.test.cutlass.http;

import io.questdb.Metrics;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;

public class HttpAlterTableTest extends AbstractTest {

    private static final String JSON_DDL_RESPONSE = "0c\r\n" +
            "{\"ddl\":\"OK\"}\r\n" +
            "00\r\n" +
            "\r\n";
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(10 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Test
    public void testAlterTableSetType() throws Exception {
        Metrics metrics = Metrics.enabled();
        testJsonQuery(2, metrics, engine -> {
            // create table
            sendAndReceiveDdl("CREATE TABLE test\n" +
                    "AS(\n" +
                    "    SELECT\n" +
                    "        x id,\n" +
                    "        timestamp_sequence(0L, 100000L) ts\n" +
                    "    FROM long_sequence(1000) x)\n" +
                    "TIMESTAMP(ts)\n" +
                    "PARTITION BY DAY");

            // execute a SELECT query
            String sql = "SELECT *\n" +
                    "FROM test t1 JOIN test t2 \n" +
                    "ON t1.id = t2.id\n" +
                    "LIMIT 1";
            sendAndReceiveBasicSelect(sql, "\r\n" +
                    "0139\r\n" +
                    "{\"query\":\"SELECT *\\nFROM test t1 JOIN test t2 \\nON t1.id = t2.id\\nLIMIT 1\",\"columns\":[{\"name\":\"id\",\"type\":\"LONG\"},{\"name\":\"ts\",\"type\":\"TIMESTAMP\"},{\"name\":\"id1\",\"type\":\"LONG\"},{\"name\":\"ts1\",\"type\":\"TIMESTAMP\"}],\"dataset\":[[1,\"1970-01-01T00:00:00.000000Z\",1,\"1970-01-01T00:00:00.000000Z\"]],\"timestamp\":1,\"count\":1}\r\n" +
                    "00\r\n" +
                    "\r\n");

            // convert table to WAL
            sendAndReceiveDdl("ALTER TABLE test SET TYPE WAL");
        });
    }

    @Test
    public void testAlterTableResume() throws Exception {
        Metrics metrics = Metrics.enabled();
        testJsonQuery(2, metrics, engine -> {
            // create table
            sendAndReceiveDdl("CREATE TABLE test\n" +
                    "AS(\n" +
                    "    SELECT\n" +
                    "        x id,\n" +
                    "        timestamp_sequence(0L, 100000L) ts\n" +
                    "    FROM long_sequence(1000) x)\n" +
                    "TIMESTAMP(ts)\n" +
                    "PARTITION BY DAY WAL");
            drainWalQueue(engine);

            // execute a SELECT query
            String sql = "SELECT *\n" +
                    "FROM test t1 JOIN test t2 \n" +
                    "ON t1.id = t2.id\n" +
                    "LIMIT 1";
            sendAndReceiveBasicSelect(sql, "\r\n" +
                    "0139\r\n" +
                    "{\"query\":\"SELECT *\\nFROM test t1 JOIN test t2 \\nON t1.id = t2.id\\nLIMIT 1\",\"columns\":[{\"name\":\"id\",\"type\":\"LONG\"},{\"name\":\"ts\",\"type\":\"TIMESTAMP\"},{\"name\":\"id1\",\"type\":\"LONG\"},{\"name\":\"ts1\",\"type\":\"TIMESTAMP\"}],\"dataset\":[[1,\"1970-01-01T00:00:00.000000Z\",1,\"1970-01-01T00:00:00.000000Z\"]],\"timestamp\":1,\"count\":1}\r\n" +
                    "00\r\n" +
                    "\r\n");

            // RESUME
            sendAndReceiveDdl("ALTER TABLE test RESUME WAL");
        });
    }

    private static void drainWalQueue(CairoEngine engine) {
        try (final ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 1, 1, null)) {
            walApplyJob.drain(0);
            new CheckWalTransactionsJob(engine).run(0);
            // run once again as there might be notifications to handle now
            walApplyJob.drain(0);
        }
    }

    private static void sendAndReceive(String request, CharSequence response) throws InterruptedException {
        new SendAndReceiveRequestBuilder()
                .withNetworkFacade(NetworkFacadeImpl.INSTANCE)
                .execute(request, response);
    }

    private static void sendAndReceiveBasicSelect(String rawSelect, String expectedBody) throws InterruptedException {
        sendAndReceive(
                "GET /query?query=" + HttpUtils.urlEncodeQuery(rawSelect) + "&count=true HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        expectedBody
        );
    }

    private static void sendAndReceiveDdl(String rawDdl) throws InterruptedException {
        sendAndReceive(
                "GET /query?query=" + HttpUtils.urlEncodeQuery(rawDdl) + "&count=true HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        JSON_DDL_RESPONSE
        );
    }

    private void testJsonQuery(int workerCount, Metrics metrics, HttpQueryTestBuilder.HttpClientCode code) throws Exception {
        final String baseDir = root;
        CairoConfiguration configuration = new DefaultTestCairoConfiguration(baseDir) {
            @Override
            public int getQueryCacheEventQueueCapacity() {
                return 1;
            }
        };
        new HttpQueryTestBuilder()
                .withWorkerCount(workerCount)
                .withTempFolder(root)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withMetrics(metrics)
                .run(configuration, code);
    }
}
