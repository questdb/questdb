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

package io.questdb.test.cutlass.qwp;

import io.questdb.PropertyKey;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Exercises QWP WebSocket queries with fiber-enabled HTTP dispatch.
 */
public class QwpEgressFiberTest extends AbstractReusedServerQwpEgressTest {

    @Test
    public void testQueryOverWebSocketOnFiber() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServerWithRetry(
                    PropertyKey.HTTP_WORKER_FIBER_ENABLED.getEnvVarName(), "true"
            )) {
                serverMain.execute("CREATE TABLE fiber_t (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO fiber_t VALUES (1, 1::TIMESTAMP), (2, 2::TIMESTAMP), (3, 3::TIMESTAMP)");
                serverMain.awaitTable("fiber_t");

                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    final long[] totalRowsSeen = {-1};
                    final long[] sumSeen = {0};
                    client.execute(
                            "SELECT x FROM fiber_t ORDER BY x",
                            new QwpColumnBatchHandler() {
                                @Override
                                public void onBatch(QwpColumnBatch batch) {
                                    Assert.assertEquals(1, batch.getColumnCount());
                                    for (int r = 0, n = batch.getRowCount(); r < n; r++) {
                                        sumSeen[0] += batch.getLongValue(0, r);
                                    }
                                }

                                @Override
                                public void onEnd(long totalRows) {
                                    totalRowsSeen[0] = totalRows;
                                }

                                @Override
                                public void onError(byte status, String message) {
                                    Assert.fail("query failed: " + message);
                                }
                            }
                    );
                    Assert.assertEquals(3, totalRowsSeen[0]);
                    Assert.assertEquals(6, sumSeen[0]);
                }
            }
        });
    }
}
