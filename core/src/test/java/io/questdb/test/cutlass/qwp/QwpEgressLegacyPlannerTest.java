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

public class QwpEgressLegacyPlannerTest extends AbstractReusedServerQwpEgressTest {
    // the legacy planner projects the join through a SelectedRecord and names the column without its table
    private static final String LEGACY_PLAN = "functions: [x+1]";
    private static final String NEW_PLAN = "functions: [a.x+1]";

    @Test
    public void testServerPropertySelectsLegacyPlanner() throws Exception {
        assertEgressPlanner("true", LEGACY_PLAN, NEW_PLAN);
    }

    @Test
    public void testServerPropertySelectsNewPlanner() throws Exception {
        assertEgressPlanner("false", NEW_PLAN, LEGACY_PLAN);
    }

    private static void assertEgressPlanner(String legacyPlannerEnabled, String expectedPlan, String otherPlan) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServerWithRetry(
                    PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED.getEnvVarName(), legacyPlannerEnabled
            )) {
                serverMain.execute("CREATE TABLE a (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("CREATE TABLE b (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO a VALUES (1, 1::TIMESTAMP), (2, 2::TIMESTAMP)");
                serverMain.execute("INSERT INTO b VALUES (2, 1::TIMESTAMP)");
                serverMain.awaitTable("a");
                serverMain.awaitTable("b");
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    final String plan = query(client, "EXPLAIN SELECT a.x + 1 FROM a JOIN b ON a.x = b.x");
                    TestUtils.assertContains(plan, expectedPlan);
                    TestUtils.assertNotContains(plan, otherPlan);
                    // twice, so that the second run takes the plan from the select cache
                    for (int i = 0; i < 2; i++) {
                        Assert.assertEquals("3\n", query(client, "SELECT (a.x + 1)::VARCHAR FROM a JOIN b ON a.x = b.x"));
                    }
                }
            }
        });
    }

    private static String query(QwpQueryClient client, String sql) {
        final StringBuilder result = new StringBuilder();
        client.execute(sql, new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                for (int r = 0; r < batch.getRowCount(); r++) {
                    result.append(batch.getString(0, r)).append('\n');
                }
            }

            @Override
            public void onEnd(long totalRows) {
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("egress error: " + message);
            }
        });
        return result.toString();
    }
}
