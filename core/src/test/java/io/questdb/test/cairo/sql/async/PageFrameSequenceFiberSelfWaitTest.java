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

package io.questdb.test.cairo.sql.async;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.cutlass.http.TestHttpClient;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

/**
 * A single network fiber can own two ordered async page-frame cursors at once, for example
 * the master and slave sides of an ASOF Light join over two Async Filters. When both
 * sequences land on the same reduce shard, the first cursor keeps the reduce-queue slot of
 * the frame it is iterating uncollected, so the shared ring cannot wrap past it. Once the
 * second cursor has collected all of its own frames and the ring is full, nothing it waits
 * for is in flight: only the first cursor, on this same fiber, can release the slot. The
 * second cursor must therefore reduce locally instead of parking, or the query sleeps
 * until the query timeout.
 * <p>
 * One reduce shard and a tiny reduce queue make the collision deterministic. A short
 * query timeout turns the hang into a fast failure.
 */
public class PageFrameSequenceFiberSelfWaitTest extends AbstractBootstrapTest {
    // trades: 1_000 rows, x % 4 cycles through S0..S3, so the filter keeps 500 master rows
    private static final String ASOF_LIGHT_COUNT = """
            SELECT /*+ asof_linear(t q) */ count() c, count(q.bid) matched
            FROM (trades WHERE sym IN ('S0', 'S1')) t
            ASOF JOIN (quotes WHERE sym IN ('S0', 'S1')) q ON (sym)
            """;
    private static final String EXPECTED_COUNT = "500";
    // the slave is a parallel GROUP BY (unordered sequence) that runs while the master
    // async filter holds an ordered reduce-queue slot on the same fiber
    private static final String FILTER_CROSS_GROUP_BY_COUNT = """
            SELECT count() c
            FROM (trades WHERE sym IN ('S0', 'S1')) t
            CROSS JOIN (SELECT sym, count() n FROM quotes WHERE sym IN ('S0', 'S1')) g
            """;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
    }

    @Test
    public void testAsOfLightOverTwoAsyncFiltersOverHttp() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    ServerMain main = startServer();
                    TestHttpClient httpClient = new TestHttpClient()
            ) {
                createTables(main);
                assertAsOfLightPlan(main);
                final CharSequenceObjHashMap<String> queryParams = new CharSequenceObjHashMap<>();
                queryParams.put("query", ASOF_LIGHT_COUNT);
                for (int i = 0; i < 3; i++) {
                    httpClient.assertGetContains(
                            "/exec",
                            "\"dataset\":[[" + EXPECTED_COUNT + "," + EXPECTED_COUNT + "]]",
                            queryParams,
                            null,
                            null,
                            main.getHttpServerPort()
                    );
                }
            }
        });
    }

    @Test
    public void testAsOfLightOverTwoAsyncFiltersOverPgWire() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain main = startServer()) {
                createTables(main);
                assertAsOfLightPlan(main);
                try (
                        Connection connection = getConnection(main);
                        Statement statement = connection.createStatement()
                ) {
                    for (int i = 0; i < 3; i++) {
                        try (ResultSet rs = statement.executeQuery(ASOF_LIGHT_COUNT)) {
                            Assert.assertTrue(rs.next());
                            Assert.assertEquals(Long.parseLong(EXPECTED_COUNT), rs.getLong(1));
                            Assert.assertEquals(Long.parseLong(EXPECTED_COUNT), rs.getLong(2));
                            Assert.assertFalse(rs.next());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testUnorderedGroupByWhileOrderedSlotHeldOverPgWire() throws Exception {
        // UnorderedPageFrameSequence parks on a full queue too, but its queue releases a slot
        // as soon as a worker picks the task up (there is no collect stage), so the waiting
        // fiber never holds what it waits for. This pins that behaviour with the same tiny
        // queues, while an ordered slot is held by the master async filter on this fiber.
        assertMemoryLeak(() -> {
            try (ServerMain main = startServer()) {
                createTables(main);
                final String plan = explain(main, FILTER_CROSS_GROUP_BY_COUNT);
                TestUtils.assertContains(plan, "Cross Join");
                TestUtils.assertContains(plan, "Group By");
                Assert.assertTrue(plan, plan.indexOf("Async ") != plan.lastIndexOf("Async "));
                try (
                        Connection connection = getConnection(main);
                        Statement statement = connection.createStatement()
                ) {
                    for (int i = 0; i < 3; i++) {
                        try (ResultSet rs = statement.executeQuery(FILTER_CROSS_GROUP_BY_COUNT)) {
                            Assert.assertTrue(rs.next());
                            // 500 master rows x 2 groups
                            Assert.assertEquals(1000, rs.getLong(1));
                            Assert.assertFalse(rs.next());
                        }
                    }
                }
            }
        });
    }

    private static void assertAsOfLightPlan(ServerMain main) throws Exception {
        final String text = explain(main, ASOF_LIGHT_COUNT);
        // Guard against a vacuous pass: the deadlock needs one fiber to interleave two
        // ordered async page-frame cursors.
        TestUtils.assertContains(text, "AsOf Join Light");
        final int first = text.indexOf("Async ");
        Assert.assertTrue(text, first > -1 && text.indexOf("Async ", first + 1) > -1);
    }

    private static String explain(ServerMain main, String sql) throws Exception {
        final StringSink plan = new StringSink();
        try (
                Connection connection = getConnection(main);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("EXPLAIN " + sql)
        ) {
            while (rs.next()) {
                plan.put(rs.getString(1)).put('\n');
            }
        }
        return plan.toString();
    }

    private static void createTables(ServerMain main) throws Exception {
        TestUtils.executeSQLViaPostgres(
                main.getConfiguration().getPGWireConfiguration().getDefaultUsername(),
                main.getConfiguration().getPGWireConfiguration().getDefaultPassword(),
                main.getPgWireServerPort(),
                // 10 hourly partitions of 100 rows: a handful of master frames
                """
                        CREATE TABLE trades AS (
                            SELECT ('S' || (x % 4))::SYMBOL sym, x::DOUBLE price, timestamp_sequence(0, 36_000_000) ts
                            FROM long_sequence(1_000)
                        ) TIMESTAMP(ts) PARTITION BY HOUR
                        """,
                // 10 hourly partitions of 10_000 rows: far more slave frames than reduce queue slots
                """
                        CREATE TABLE quotes AS (
                            SELECT ('S' || (x % 4))::SYMBOL sym, x::DOUBLE bid, timestamp_sequence(0, 360_000) ts
                            FROM long_sequence(100_000)
                        ) TIMESTAMP(ts) PARTITION BY HOUR
                        """
        );
    }

    private static Connection getConnection(ServerMain main) throws Exception {
        return getConnection(
                main.getConfiguration().getPGWireConfiguration().getDefaultUsername(),
                main.getConfiguration().getPGWireConfiguration().getDefaultPassword(),
                main.getPgWireServerPort()
        );
    }

    private static ServerMain startServer() {
        final ServerMain main = startWithEnvVariables(
                // one shard: both sequences of the join always share a reduce ring
                PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT.getEnvVarName(), "1",
                // fewer slots than the slave has frames
                PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY.getEnvVarName(), "4",
                PropertyKey.CAIRO_UNORDERED_PAGE_FRAME_REDUCE_QUEUE_CAPACITY.getEnvVarName(), "2",
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS.getEnvVarName(), "100",
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), "1000",
                PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS.getEnvVarName(), "100",
                PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), "1000",
                // a hang fails fast instead of blocking the suite for the default 60s
                PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "10s"
        );
        try {
            // The defect needs the query to run on a network fiber that is foreign to the
            // fiber runtime owning the page-frame reduce dispatcher.
            Assert.assertNotNull(main.getEngine().getMessageBus().getPageFrameReduceDispatcher());
            Assert.assertEquals(1, main.getEngine().getMessageBus().getPageFrameReduceShardCount());
            return main;
        } catch (Throwable th) {
            main.close();
            throw th;
        }
    }
}
