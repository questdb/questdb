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

package io.questdb.test.metrics;


import io.questdb.griffin.SqlException;
import io.questdb.metrics.QueryTracingJob;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import junit.framework.AssertionFailedError;
import org.junit.Test;

public class QueryTracingTest extends AbstractCairoTest {
    @Test
    public void testTracing() throws Exception {
        try (WorkerPool workerPool = new WorkerPool(() -> 1)) {
            QueryTracingJob.assignToPool(workerPool, engine);
            workerPool.start(LOG);
            String exampleQuery = "SELECT table_name FROM tables()";
            assertSql("table_name\n", exampleQuery);
            String fullName = engine.getConfiguration().getSystemTableNamePrefix() + QueryTracingJob.TABLE_NAME;
            for (int i = 0; ; i++) {
                Thread.sleep(100);
                try {
                    assertSql(String.format("query_text\n%s\n", exampleQuery),
                            String.format("SELECT query_text from '%s' LIMIT 1", fullName));
                    break;
                } catch (SqlException | AssertionFailedError e) {
                    if (i == 100) {
                        throw e;
                    }
                }
            }
        }
    }
}
