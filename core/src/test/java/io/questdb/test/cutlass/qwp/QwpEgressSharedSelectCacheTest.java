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

package io.questdb.test.cutlass.qwp;

import io.questdb.PropertyKey;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.metrics.Counter;
import io.questdb.std.Os;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The QWP egress select cache is one cache for all network workers: a connection's requests are
 * served by whichever worker picks them up, and a plan compiled by one worker must be a hit on
 * every other. With a cache per worker, each worker that first served a request recompiled it
 * (2-4 ms for a window query), which showed up as random slow warm runs.
 */
public class QwpEgressSharedSelectCacheTest extends AbstractReusedServerQwpEgressTest {

    @Test
    public void testPlanCompiledOnOneWorkerIsHitOnAllWorkers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServerWithRetry(
                    PropertyKey.HTTP_QUERY_CACHE_ENABLED.getEnvVarName(), "true",
                    PropertyKey.METRICS_ENABLED.getEnvVarName(), "true",
                    PropertyKey.SHARED_NETWORK_WORKER_COUNT.getEnvVarName(), "4",
                    // per-worker jobs, so each of the 4 workers serves requests with its own processor
                    PropertyKey.SHARED_NETWORK_WORKER_FIBER_ENABLED.getEnvVarName(), "false",
                    PropertyKey.HTTP_WORKER_FIBER_ENABLED.getEnvVarName(), "false"
            )) {
                serverMain.execute("CREATE TABLE sc (sym SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO sc VALUES ('a', 1, 1::TIMESTAMP), ('b', 2, 2::TIMESTAMP), ('a', 3, 3::TIMESTAMP)");
                serverMain.awaitTable("sc");
                final String sql = "SELECT sum(v) s FROM sc WHERE sym = 'a'";
                final Counter misses = serverMain.getEngine().getMetrics().jsonQueryMetrics().cacheMissCounter();
                final Counter hits = serverMain.getEngine().getMetrics().jsonQueryMetrics().cacheHitCounter();
                final long misses0 = misses.getValue();
                final long hits0 = hits.getValue();
                final int connections = 4;
                final int rounds = 40;
                final QwpQueryClient[] clients = new QwpQueryClient[connections];
                try {
                    for (int c = 0; c < connections; c++) {
                        clients[c] = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT);
                        clients[c].connect();
                    }
                    for (int r = 0; r < rounds; r++) {
                        for (int c = 0; c < connections; c++) {
                            Assert.assertEquals(4L, querySum(clients[c], sql));
                        }
                        // idle gaps of 0-3 ms let the workers park, so the next request is
                        // picked up by whichever of them wakes first
                        Os.sleep(r % 4);
                    }
                } finally {
                    for (QwpQueryClient client : clients) {
                        if (client != null) {
                            client.close();
                        }
                    }
                }
                // exactly one compile: the first request; every other one, on any worker, is a hit
                Assert.assertEquals("select cache misses", 1, misses.getValue() - misses0);
                Assert.assertEquals("select cache hits", connections * rounds - 1, hits.getValue() - hits0);
            }
        });
    }

    private static long querySum(QwpQueryClient client, String sql) {
        final long[] out = {Long.MIN_VALUE, 0};
        client.execute(sql, new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                if (batch.getRowCount() > 0) {
                    out[0] = batch.getLongValue(0, 0);
                }
                out[1] += batch.getRowCount();
            }

            @Override
            public void onEnd(long totalRows) {
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("SELECT failed (status=" + status + "): " + message);
            }
        });
        Assert.assertEquals(1L, out[1]);
        return out[0];
    }
}
