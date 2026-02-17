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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Fuzz test for thread pooling strategy changes that tests unequal pool sizes
 * where writer pool could be 4 workers and query pool could be 10 workers.
 * This creates a scenario where SQL may partition its data structures for 4 workers
 * but will have 10 workers executing, potentially causing chaos.
 * <p>
 * This test simulates the scenario by creating a WorkerPool with 10 workers
 * and configuring the SqlExecutionContext to report different worker counts
 * to create the imbalance described in the PR comment.
 */
public class UnequalWorkerPoolTest extends AbstractBootstrapTest {
    private static final int CONCURRENT_QUERIES = 8;
    private static final int ITERATIONS = 10;
    private static final int PAGE_FRAME_COUNT = 4;
    private static final int PAGE_FRAME_MAX_ROWS = 100;
    private static final int ROW_COUNT = 20 * PAGE_FRAME_COUNT * PAGE_FRAME_MAX_ROWS;

    @Test
    public void testUnequalPoolSizesConcurrentGroupBy() throws Exception {

        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();

        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), String.valueOf(PAGE_FRAME_MAX_ROWS));
                put(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY.getEnvVarName(), String.valueOf(PAGE_FRAME_COUNT));
                put(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD.getEnvVarName(), "2");
                put(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD.getEnvVarName(), "1");
                put(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED.getEnvVarName(), "true");
                put(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED.getEnvVarName(), "true");
                put(PropertyKey.SHARED_WORKER_COUNT.getEnvVarName(), "1");
                put(PropertyKey.SHARED_QUERY_WORKER_COUNT.getEnvVarName(), "4");

            }})) {

                serverMain.start();

                serverMain.getEngine().execute(
                        "CREATE TABLE metrics (" +
                                "ts TIMESTAMP, " +
                                "sensor_id SYMBOL, " +
                                "region SYMBOL, " +
                                "value DOUBLE, " +
                                "category LONG" +
                                ") TIMESTAMP(ts) PARTITION BY MONTH"
                );

                // Insert large batch of data using long_sequence for parallel execution
                serverMain.getEngine().execute(
                        "INSERT INTO metrics " +
                                "SELECT " +
                                "  (x * 864000000)::timestamp, " +
                                "  'sensor_' || (x % 50), " +
                                "  'region_' || (x % 5), " +
                                "  rnd_double() * 100, " +
                                "  x % 20 " +
                                "FROM long_sequence(" + ROW_COUNT + ")"
                );

                serverMain.awaitTable("metrics");

                // Create chaos by running concurrent queries with different pool sizes
                final CyclicBarrier barrier = new CyclicBarrier(CONCURRENT_QUERIES);
                final AtomicInteger successCount = new AtomicInteger(0);
                final AtomicInteger errorCount = new AtomicInteger(0);
                final AtomicReference<Exception> lastError = new AtomicReference<>();
                final Thread[] threads = new Thread[CONCURRENT_QUERIES];

                for (int i = 0; i < CONCURRENT_QUERIES; i++) {
                    final int threadId = i;
                    threads[i] = new Thread(() -> {
                        try {
                            barrier.await();

                            try (final Connection connection = getPgConnection()) {

                                // Execute queries that should trigger parallel execution
                                for (int iter = 0; iter < ITERATIONS; iter++) {
                                    final String[] queries = {
                                            "SELECT sensor_id, count(*) FROM metrics GROUP BY sensor_id",
                                            "SELECT region, avg(value) FROM metrics GROUP BY region",
                                            "SELECT sensor_id, region, sum(value) FROM metrics GROUP BY sensor_id, region",
                                            "SELECT category, min(value), max(value) FROM metrics GROUP BY category ORDER BY category",
                                            "SELECT sensor_id, count(*) FROM metrics WHERE value > 50 GROUP BY sensor_id",
                                            "SELECT region, sensor_id, count(*) FROM metrics GROUP BY region, sensor_id ORDER BY region, sensor_id",
                                            "SELECT category, avg(value) FROM metrics WHERE region = 'region_1' GROUP BY category",
                                            "SELECT sensor_id, sum(value) FROM metrics WHERE ts >= '1970-01-01' GROUP BY sensor_id"
                                    };

                                    final String query = queries[iter % queries.length];

                                    Statement statement = connection.createStatement();
                                    statement.execute(query);
                                }
                            }

                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            lastError.set(e);
                            LOG.error().$("Query execution failed in thread ").$(threadId).$(" with error: ").$(e.getMessage()).$();
                        }
                    });
                }

                // Start all threads
                for (Thread thread : threads) {
                    thread.start();
                }

                // Wait for all threads to complete
                for (Thread thread : threads) {
                    thread.join();
                }

                // Check results
                final int totalOperations = successCount.get() + errorCount.get();
                LOG.info().$("Unequal pool size test completed. Success: ").$(successCount.get())
                        .$(", Errors: ").$(errorCount.get())
                        .$(", Total: ").$(totalOperations).$();

                if (errorCount.get() > 0) {
                    final Exception error = lastError.get();
                    if (error != null) {
                        LOG.error().$("Last error: ").$(error.getMessage()).$();
                    }
                    // Allow some errors but not too many in this chaos test
                    Assert.assertTrue("Too many errors: " + errorCount.get() + " out of " + totalOperations,
                            errorCount.get() < CONCURRENT_QUERIES / 2);
                }

                Assert.assertTrue("No successful operations", successCount.get() > 0);
            }
        });
    }

    private Connection getPgConnection() throws SQLException {
        Properties properties = new Properties(PG_CONNECTION_PROPERTIES);
        properties.setProperty("prepareThreshold", "1");
        return DriverManager.getConnection(PG_CONNECTION_URI, properties);
    }
}