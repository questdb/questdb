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


import io.questdb.PropertyKey;
import io.questdb.griffin.SqlException;
import io.questdb.metrics.QueryTracingJob;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

public class QueryTracingTest extends AbstractCairoTest {

    private String queryTraceTableName;

    @Before
    public void setup() throws SqlException {
        node1.getConfigurationOverrides().setProperty(PropertyKey.QUERY_TRACING_ENABLED, true);
        queryTraceTableName = engine.getConfiguration().getSystemTableNamePrefix() + QueryTracingJob.TABLE_NAME;
        engine.execute("DROP TABLE IF EXISTS '" + queryTraceTableName + "'");
    }

    @Test
    public void testQueryTraceFunction() throws Exception {
        try (WorkerPool workerPool = new WorkerPool(() -> 1);
             QueryTracingJob job = new QueryTracingJob(engine)
        ) {
            workerPool.assign(job);
            workerPool.start(LOG);
            String exampleQuery = "SELECT table_name FROM tables()";
            assertSql("table_name\n", exampleQuery);
            int sleepMillis = 100;
            while (true) {
                Thread.sleep(sleepMillis);
                try {
                    assertSql(
                            String.format("%s\n%s\n", QueryTracingJob.COLUMN_QUERY_TEXT, exampleQuery),
                            String.format("SELECT %s from %s() WHERE %s='%s' LIMIT 1",
                                    QueryTracingJob.COLUMN_QUERY_TEXT,
                                    QueryTracingJob.TABLE_NAME,
                                    QueryTracingJob.COLUMN_QUERY_TEXT,
                                    exampleQuery
                            ));
                    break;
                } catch (SqlException | AssertionError e) {
                    if (sleepMillis >= 6400) {
                        throw e;
                    }
                    sleepMillis *= 2;
                }
            }
        }
    }

    @Test
    public void testQueryTracingTable() throws Exception {
        try (WorkerPool workerPool = new WorkerPool(() -> 1);
             QueryTracingJob job = new QueryTracingJob(engine)
        ) {
            workerPool.assign(job);
            workerPool.start(LOG);
            String exampleQuery = "SELECT table_name FROM tables()";
            assertSql("table_name\n", exampleQuery);
            int sleepMillis = 100;
            while (true) {
                Thread.sleep(sleepMillis);
                try {
                    assertSql(
                            String.format("%s\n%s\n", QueryTracingJob.COLUMN_QUERY_TEXT, exampleQuery),
                            String.format("SELECT %s from %s WHERE %s='%s' LIMIT 1",
                                    QueryTracingJob.COLUMN_QUERY_TEXT,
                                    queryTraceTableName,
                                    QueryTracingJob.COLUMN_QUERY_TEXT,
                                    exampleQuery
                            ));
                    break;
                } catch (SqlException | AssertionError e) {
                    if (sleepMillis >= 6400) {
                        throw e;
                    }
                    sleepMillis *= 2;
                }
            }
        }
    }
}
