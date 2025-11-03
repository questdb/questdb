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
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.cutlass.pgwire.BasePGTest.assertResultSet;
import static io.questdb.test.tools.TestUtils.unchecked;

public class ServerMainQueryTimeoutTest extends AbstractBootstrapTest {
    private Rnd rnd;
    private boolean useQueryCache;

    @Before
    public void setUp() {
        super.setUp();
        rnd = TestUtils.generateRandom(LOG);
        useQueryCache = rnd.nextBoolean();
        LOG.info().$("query cache: ").$(useQueryCache).$();
        unchecked(() -> createDummyConfiguration(
                PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED + "=true",
                PropertyKey.SHARED_WORKER_COUNT + "=4",
                PropertyKey.PG_WORKER_COUNT + "=4",
                PropertyKey.WAL_APPLY_WORKER_COUNT + "=1",
                PropertyKey.PG_SELECT_CACHE_ENABLED + "=" + useQueryCache,
                // we want more reduce tasks, hence smaller page frames
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS + "=1000",
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS + "=10000",
                // with 10ms timeout queries have a small chance to execute successfully
                PropertyKey.QUERY_TIMEOUT + "=10ms",
                // the scoreboard has to be small to simplify detecting table reader leak
                PropertyKey.CAIRO_O3_TXN_SCOREBOARD_ENTRY_COUNT + "16"
        ));
        dbPath.parent().$();
    }

    @Test
    public void testQueryTimeout() throws Exception {
        try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
            put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
        }})) {
            serverMain.start();

            final long rowCount = 10_000_000;
            try (
                    Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                    Statement stmt = conn.createStatement()
            ) {
                stmt.execute(
                        "CREATE TABLE tab as (" +
                                "select (x * 864_000_000)::timestamp ts, ('k' || (x % 5))::symbol key, x::double price, x::long quantity" +
                                " from long_sequence(" + rowCount + ")" +
                                ") timestamp (ts) PARTITION BY MONTH WAL;"
                );
            }

            TestUtils.drainWalQueue(serverMain.getEngine());

            final int nThreads = 4;
            final int nIterations = 100;

            final CyclicBarrier startBarrier = new CyclicBarrier(nThreads);
            final SOCountDownLatch doneLatch = new SOCountDownLatch(nThreads);
            final AtomicInteger errors = new AtomicInteger();
            final AtomicInteger timeouts = new AtomicInteger();
            for (int t = 0; t < nThreads; t++) {
                new Thread(() -> {
                    final Rnd threadRnd = new Rnd(rnd.getSeed0(), rnd.getSeed1());
                    final StringSink sink = new StringSink();
                    try {
                        startBarrier.await();
                        try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                            for (int i = 0, localLeaks = 0; i < nIterations && localLeaks < 5; i++) {
                                final String query = "SELECT * FROM tab WHERE key = 'k0' or key = 'k3' LIMIT 1_999_990, 2_000_000";
                                final StringBuilder sb = new StringBuilder(query);
                                if (!useQueryCache) {
                                    // append a random trailing comment, so that the query cache doesn't kick in
                                    sb.append(" -- ");
                                    sb.append(threadRnd.nextPositiveLong());
                                }

                                try (
                                        PreparedStatement stmt = conn.prepareStatement(sb.toString());
                                        ResultSet rs = stmt.executeQuery()
                                ) {
                                    sink.clear();
                                    assertResultSet(
                                            "ts[TIMESTAMP],key[VARCHAR],price[DOUBLE],quantity[BIGINT]\n" +
                                                    "2106-11-23 18:43:12.0,k3,4999978.0,4999978\n" +
                                                    "2106-11-23 19:12:00.0,k0,4999980.0,4999980\n" +
                                                    "2106-11-23 19:55:12.0,k3,4999983.0,4999983\n" +
                                                    "2106-11-23 20:24:00.0,k0,4999985.0,4999985\n" +
                                                    "2106-11-23 21:07:12.0,k3,4999988.0,4999988\n" +
                                                    "2106-11-23 21:36:00.0,k0,4999990.0,4999990\n" +
                                                    "2106-11-23 22:19:12.0,k3,4999993.0,4999993\n" +
                                                    "2106-11-23 22:48:00.0,k0,4999995.0,4999995\n" +
                                                    "2106-11-23 23:31:12.0,k3,4999998.0,4999998\n" +
                                                    "2106-11-24 00:00:00.0,k0,5000000.0,5000000\n",
                                            sink,
                                            rs
                                    );
                                } catch (PSQLException e) {
                                    // timeouts are fine
                                    TestUtils.assertContains(e.getMessage(), "timeout, query aborted");
                                    localLeaks++;
                                    timeouts.incrementAndGet();
                                }

                                // Bump txn, so that if we have a reader leak we notice that.
                                try (PreparedStatement insertStmt = conn.prepareStatement("INSERT INTO tab VALUES ('3000-01-01T00:00:00.000000Z', 'k42', 42, 42);")) {
                                    insertStmt.execute();
                                }
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
            Assert.assertTrue(timeouts.get() > 0);
        }
    }
}
