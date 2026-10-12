/*+*****************************************************************************
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

import io.questdb.PropertyKey;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class HttpFilteredScanFiberTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testHashJoinOverTwoFilteredScans() throws Exception {
        assertExecReturns(
                "SELECT t.k FROM t JOIN s ON s.k = t.k WHERE t.vt IS NULL AND s.ct = 'u'",
                """
                        QUERY PLAN
                        SelectedRecord
                            Hash Join Light
                              condition: s.k=t.k
                                Async JIT Filter workers: 1
                                  filter: null=vt
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                                Hash
                                    Async JIT Filter workers: 1
                                      filter: ct=u
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: s
                        """,
                "\"dataset\":[[7],[17],[27],[37]],\"count\":4"
        );
    }

    @Test
    public void testUnionAllOfTwoFilteredScans() throws Exception {
        assertExecReturns(
                "SELECT sum(k) FROM (SELECT k FROM s WHERE ct = 'u' UNION ALL SELECT k FROM t WHERE vt IS NULL)",
                """
                        QUERY PLAN
                        GroupBy vectorized: false
                          values: [sum(k)]
                            Union All
                                SelectedRecord
                                    Async JIT Filter workers: 1
                                      filter: ct=u
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: s
                                SelectedRecord
                                    Async JIT Filter workers: 1
                                      filter: null=vt
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                        """,
                "\"dataset\":[[908]],\"count\":1"
        );
    }

    private static void assertExecReturns(String query, String expectedPlan, String expectedResult) throws Exception {
        assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_WORKER_FIBER_ENABLED.getEnvVarName(), "true",
                    PropertyKey.SHARED_QUERY_WORKER_FIBER_ENABLED.getEnvVarName(), "true",
                    PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT.getEnvVarName(), "1",
                    PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY.getEnvVarName(), "16"
            )) {
                serverMain.execute("""
                        CREATE TABLE t AS (
                            SELECT x AS k, NULL::TIMESTAMP AS vt, timestamp_sequence(0, 86_400_000_000) AS ts
                            FROM long_sequence(40)
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """);
                serverMain.execute("CREATE TABLE s AS (SELECT k, 'u' AS ct FROM t WHERE k % 10 = 7)");
                serverMain.assertSql("EXPLAIN " + query, expectedPlan);

                final HttpRequest request = HttpRequest.newBuilder(URI.create(
                                "http://127.0.0.1:" + serverMain.getHttpServerPort()
                                        + "/exec?query=" + URLEncoder.encode(query, StandardCharsets.UTF_8)
                        ))
                        .timeout(Duration.ofSeconds(10))
                        .build();
                final HttpClient client = HttpClient.newHttpClient();
                for (int i = 0; i < 3; i++) {
                    final HttpResponse<String> response;
                    try {
                        response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    } catch (HttpTimeoutException e) {
                        throw new AssertionError("query stalled [run=" + i + ", query=" + query + ']', e);
                    }
                    Assert.assertEquals(200, response.statusCode());
                    TestUtils.assertContains(response.body(), expectedResult);
                }
            }
        });
    }
}
