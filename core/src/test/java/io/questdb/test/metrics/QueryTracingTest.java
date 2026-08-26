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

package io.questdb.test.metrics;


import io.questdb.PropertyKey;
import io.questdb.griffin.SqlException;
import io.questdb.metrics.QueryTracingJob;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.metrics.QueryTracingJob.*;

public class QueryTracingTest extends AbstractCairoTest {

    @Before
    public void setup() throws SqlException {
        node1.getConfigurationOverrides().setProperty(PropertyKey.QUERY_TRACING_ENABLED, true);
        engine.execute("DROP TABLE IF EXISTS '" + TABLE_NAME + "'");
    }

    @Test
    public void testMigrationAddsTimingColumns() throws Exception {
        assertMemoryLeak(() -> {
            engine.execute(
                    "CREATE TABLE '_query_trace' (" +
                            "ts TIMESTAMP, query_text VARCHAR, execution_micros LONG, principal VARCHAR" +
                            ") TIMESTAMP(ts) PARTITION BY HOUR TTL 1 DAY BYPASS WAL"
            );
            try (QueryTracingJob ignore = new QueryTracingJob(engine)) {
                assertQuery("SELECT \"column\" FROM (SHOW COLUMNS FROM '_query_trace') WHERE \"column\" IN ('wait_micros', 'first_row_micros')")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("column\nwait_micros\nfirst_row_micros\n");
            }

            engine.execute("DROP TABLE '_query_trace'");
            engine.execute(
                    "CREATE TABLE '_query_trace' (" +
                            "ts TIMESTAMP, query_text VARCHAR, execution_micros LONG, principal VARCHAR, wait_micros LONG" +
                            ") TIMESTAMP(ts) PARTITION BY HOUR TTL 1 DAY BYPASS WAL"
            );
            try (QueryTracingJob ignore = new QueryTracingJob(engine)) {
                assertQuery("SELECT \"column\" FROM (SHOW COLUMNS FROM '_query_trace') WHERE \"column\" = 'first_row_micros'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("column\nfirst_row_micros\n");
            }

            engine.execute("DROP TABLE '_query_trace'");
            engine.execute(
                    "CREATE TABLE '_query_trace' (" +
                            "ts TIMESTAMP, query_text VARCHAR, execution_micros LONG, principal VARCHAR, first_row_micros LONG" +
                            ") TIMESTAMP(ts) PARTITION BY HOUR TTL 1 DAY BYPASS WAL"
            );
            try (QueryTracingJob ignore = new QueryTracingJob(engine)) {
                assertQuery("SELECT \"column\" FROM (SHOW COLUMNS FROM '_query_trace') WHERE \"column\" = 'wait_micros'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("column\nwait_micros\n");
            }
        });
    }

    @Test
    public void testQueryTracing() throws Exception {
        try (WorkerPool workerPool = new WorkerPool(() -> 1);
             QueryTracingJob job = new QueryTracingJob(engine)
        ) {
            workerPool.assign(job);
            workerPool.start(LOG);
            try {
                String exampleQuery = "SELECT table_name FROM tables()";
                assertQuery(exampleQuery)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("table_name\n");
                int sleepMillis = 100;
                while (true) {
                    Thread.sleep(sleepMillis);
                    try {
                        assertQuery(String.format("SELECT %s, %s from %s WHERE %s='%s' LIMIT 1",
                                COLUMN_QUERY_TEXT,
                                COLUMN_PRINCIPAL,
                                TABLE_NAME,
                                COLUMN_QUERY_TEXT,
                                exampleQuery
                        ))
                                .noLeakCheck()
                                .returns(String.format("%s\t%s\n%s\tadmin\n", COLUMN_QUERY_TEXT, COLUMN_PRINCIPAL, exampleQuery));
                        break;
                    } catch (SqlException | AssertionError e) {
                        if (sleepMillis >= 6400) {
                            throw e;
                        }
                        sleepMillis *= 2;
                    }
                }
            } finally {
                workerPool.halt();
            }
        }
    }

    @Test
    public void testTraceRowCarriesTimingColumns() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "SELECT 1 AS x";
            try (WorkerPool workerPool = new WorkerPool(() -> 1);
                 QueryTracingJob job = new QueryTracingJob(engine)
            ) {
                workerPool.assign(job);
                workerPool.start(LOG);
                try {
                    assertQuery(query)
                            .noLeakCheck()
                            .expectSize()
                            .returns("x\n1\n");
                    int sleepMillis = 100;
                    while (true) {
                        Thread.sleep(sleepMillis);
                        try {
                            assertQuery("SELECT count() > 0 AS has_trace FROM _query_trace WHERE query_text = '" + query + "' AND wait_micros = 0 AND first_row_micros >= 0 AND first_row_micros <= execution_micros")
                                    .noLeakCheck()
                                    .noRandomAccess()
                                    .expectSize()
                                    .returns("has_trace\ntrue\n");
                            break;
                        } catch (SqlException | AssertionError e) {
                            if (sleepMillis >= 6400) {
                                throw e;
                            }
                            sleepMillis *= 2;
                        }
                    }
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    @Test
    public void testZeroRowQueryHasNullFirstRow() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "SELECT table_name FROM tables() WHERE table_name = 'no_such'";
            try (WorkerPool workerPool = new WorkerPool(() -> 1);
                 QueryTracingJob job = new QueryTracingJob(engine)
            ) {
                workerPool.assign(job);
                workerPool.start(LOG);
                try {
                    assertQuery(query)
                            .noLeakCheck()
                            .noRandomAccess()
                            .returns("table_name\n");
                    int sleepMillis = 100;
                    while (true) {
                        Thread.sleep(sleepMillis);
                        try {
                            assertQuery("SELECT count() > 0 AS has_trace FROM _query_trace WHERE query_text = '" + query.replace("'", "''") + "' AND first_row_micros IS NULL AND wait_micros = 0")
                                    .noLeakCheck()
                                    .noRandomAccess()
                                    .expectSize()
                                    .returns("has_trace\ntrue\n");
                            break;
                        } catch (SqlException | AssertionError e) {
                            if (sleepMillis >= 6400) {
                                throw e;
                            }
                            sleepMillis *= 2;
                        }
                    }
                } finally {
                    workerPool.halt();
                }
            }
        });
    }
}
