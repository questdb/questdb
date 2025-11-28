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

package io.questdb.test;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.cutlass.pgwire.BasePGTest.assertResultSet;
import static io.questdb.test.tools.TestUtils.unchecked;

/**
 * E2E work stealing stress test.
 */
public class ServerMainWorkStealingTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration(
                // Force enable parallel GROUP BY and filter.
                PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED + "=true",
                PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED + "=true",
                // Use only single worker thread to maximize chances of work stealing.
                PropertyKey.SHARED_WORKER_COUNT + "=1",
                PropertyKey.PG_WORKER_COUNT + "=8",
                PropertyKey.PG_SELECT_CACHE_ENABLED + "=true",
                // Maximize contention over the reduce queue.
                PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT + "=1",
                PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY + "=32",
                // Make the only shared worker sleepy.
                PropertyKey.SHARED_WORKER_YIELD_THRESHOLD + "=1",
                PropertyKey.SHARED_WORKER_NAP_THRESHOLD + "=2",
                PropertyKey.SHARED_WORKER_SLEEP_THRESHOLD + "=3",
                PropertyKey.CIRCUIT_BREAKER_THROTTLE + "=5",
                PropertyKey.QUERY_TIMEOUT + "=150000"
        ));
        dbPath.parent().$();
    }

    @Test
    public void testWorkStealing() throws Exception {
        try (ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();

            try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                try (Statement statement = conn.createStatement()) {
                    statement.execute(
                            "CREATE TABLE tab as ( " +
                                    "  select (x * 86400000)::timestamp ts, ('k' || (x % 5))::symbol key, x::double price, x::long quantity " +
                                    "  from long_sequence(10000) " +
                                    ") timestamp (ts) PARTITION BY DAY;"
                    );
                }
            }

            final int nThreads = 8;
            final int nIterations = 50;

            final String query = "SELECT * FROM tab WHERE key = 'k3' LIMIT 10;";
            final String expectedResult = """
                    ts[TIMESTAMP],key[VARCHAR],price[DOUBLE],quantity[BIGINT]
                    1970-01-01 00:04:19.2,k3,3.0,3
                    1970-01-01 00:11:31.2,k3,8.0,8
                    1970-01-01 00:18:43.2,k3,13.0,13
                    1970-01-01 00:25:55.2,k3,18.0,18
                    1970-01-01 00:33:07.2,k3,23.0,23
                    1970-01-01 00:40:19.2,k3,28.0,28
                    1970-01-01 00:47:31.2,k3,33.0,33
                    1970-01-01 00:54:43.2,k3,38.0,38
                    1970-01-01 01:01:55.2,k3,43.0,43
                    1970-01-01 01:09:07.2,k3,48.0,48
                    """;

            final CyclicBarrier startBarrier = new CyclicBarrier(nThreads);
            final SOCountDownLatch doneLatch = new SOCountDownLatch(nThreads);
            final AtomicInteger errors = new AtomicInteger();
            for (int t = 0; t < nThreads; t++) {
                new Thread(() -> {
                    final StringSink sink = new StringSink();
                    try {
                        startBarrier.await();

                        for (int i = 0; i < nIterations; i++) {
                            try (
                                    Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                                    PreparedStatement stmt = conn.prepareStatement(query);
                                    ResultSet rs = stmt.executeQuery()
                            ) {
                                sink.clear();
                                assertResultSet(expectedResult, sink, rs);
                            }
                        }
                    } catch (Throwable th) {
                        errors.incrementAndGet();
                        th.printStackTrace(System.out);
                    } finally {
                        doneLatch.countDown();
                    }
                }).start();
            }

            doneLatch.await();
            Assert.assertEquals(0, errors.get());
        }
    }
}
