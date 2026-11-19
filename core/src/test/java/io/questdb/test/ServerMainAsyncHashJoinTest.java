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

package io.questdb.test;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinTupleRing;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.ParallelHashJoinTestUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static io.questdb.test.tools.TestUtils.unchecked;

/**
 * The parallel hash join at the server's defaults, which a PropServerConfiguration derives from the
 * query workers: on when there are query workers, a dispatch limit of {@code min(W, 32)}, a reduce
 * queue of {@code min(4 W, 256)} rounded up to a power of two, page frames of up to a million rows,
 * builds of 300_000 rows or more in rounds, and a tuple ring budget that keeps the whole window for
 * stacks of up to five joins. A star over two million probe rows, one of whose builds builds in
 * rounds, runs the operator through PG wire and returns the rows that the ordinary plan returns on
 * the same server, and a stack of five joins opens the window that the configuration allows.
 */
public class ServerMainAsyncHashJoinTest extends AbstractBootstrapTest {
    // A stack of three joins whose first build, of 400_000 rows, builds in rounds; the probe filter
    // keeps one probe row in a thousand.
    private static final String STAR = "SELECT f.v, d1.w, d2.w2, d3.w3 FROM f LEFT JOIN d1 ON f.k = d1.k JOIN d2 ON f.l = d2.l"
            + " LEFT JOIN d3 ON f.m = d3.m WHERE f.v % 1_000 = 0";
    // A stack of five joins over every probe row.
    private static final String STACK5 = "SELECT f.v, d1.w, d2.w2, d3.w3, d4.w4, d5.w5 FROM f LEFT JOIN d1 ON f.k = d1.k"
            + " LEFT JOIN d2 ON f.l = d2.l LEFT JOIN d3 ON f.m = d3.m LEFT JOIN d4 ON f.l = d4.l LEFT JOIN d5 ON f.m = d5.m";

    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testHashJoinAloneStartsQueryWorkers() throws Exception {
        // server.conf turns every parallel SQL kind off but the parallel hash join, which then still
        // runs on the shared query workers, as the configuration documents.
        createDummyConfiguration(
                PropertyKey.SHARED_QUERY_WORKER_COUNT.getPropertyPath() + "=4",
                PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED.getPropertyPath() + "=false",
                PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED.getPropertyPath() + "=false",
                PropertyKey.CAIRO_SQL_PARALLEL_HORIZON_JOIN_ENABLED.getPropertyPath() + "=false",
                PropertyKey.CAIRO_SQL_PARALLEL_READ_PARQUET_ENABLED.getPropertyPath() + "=false",
                PropertyKey.CAIRO_SQL_PARALLEL_TOP_K_ENABLED.getPropertyPath() + "=false",
                PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_JOIN_ENABLED.getPropertyPath() + "=false",
                PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_ENABLED.getPropertyPath() + "=true"
        );
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();
                Assert.assertTrue(serverMain.getEngine().getConfiguration().isSqlParallelHashJoinEnabled());
                Assert.assertEquals(4, serverMain.getConfiguration().getSharedWorkerPoolQueryConfiguration().getWorkerCount());
                try (
                        Connection connection = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement statement = connection.createStatement()
                ) {
                    statement.execute("CREATE TABLE f AS (SELECT (x % 10)::INT k, x v, timestamp_sequence('2024-01-01', 1_000_000) ts"
                            + " FROM long_sequence(100)) TIMESTAMP(ts) PARTITION BY DAY");
                    statement.execute("CREATE TABLE d AS (SELECT x::INT k, x w FROM long_sequence(5))");
                    final StringSink plan = new StringSink();
                    try (ResultSet resultSet = statement.executeQuery("EXPLAIN SELECT f.v, d.w FROM f JOIN d ON f.k = d.k")) {
                        while (resultSet.next()) {
                            plan.put(resultSet.getString(1)).put('\n');
                        }
                    }
                    TestUtils.assertContains(plan, "Async Hash Join workers: 4");
                    // Keys 1 to 5 match d, 10 fact rows each.
                    try (ResultSet resultSet = statement.executeQuery("SELECT count(*), sum(d.w) FROM f JOIN d ON f.k = d.k")) {
                        Assert.assertTrue(resultSet.next());
                        Assert.assertEquals(50, resultSet.getLong(1));
                        Assert.assertEquals(150, resultSet.getLong(2));
                    }
                }
            }
        });
    }

    @Test
    public void testStarJoinAtServerDefaults() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();
                final CairoConfiguration configuration = engine.getConfiguration();
                final int workerCount = serverMain.getConfiguration().getSharedWorkerPoolQueryConfiguration().getWorkerCount();
                Assert.assertTrue(workerCount > 0);
                Assert.assertTrue(configuration.isSqlParallelHashJoinEnabled());
                final int dispatchLimit = configuration.getSqlParallelFilterDispatchLimit();
                final int queueCapacity = configuration.getPageFrameReduceQueueCapacity();
                Assert.assertEquals(Math.min(workerCount, 32), dispatchLimit);
                Assert.assertEquals(Numbers.ceilPow2(Math.min(4 * workerCount, 256)), queueCapacity);
                // Probe frames of at most 100_000 rows, below the page frames' 1_000_000, which the
                // task cap and the ring budget follow.
                Assert.assertEquals(1_000_000, configuration.getSqlPageFrameMaxRows());
                Assert.assertEquals(100_000, configuration.getSqlParallelHashJoinPageFrameMaxRows());
                Assert.assertEquals(100_000, configuration.getSqlParallelHashJoinTaskOutputMaxRows());
                Assert.assertEquals(
                        (long) Math.min(dispatchLimit, queueCapacity) * 100_000 * 6 * Long.BYTES,
                        configuration.getSqlParallelHashJoinTupleRingMaxSize()
                );
                try (
                        Connection connection = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        Statement statement = connection.createStatement()
                ) {
                    statement.execute("CREATE TABLE f AS (SELECT (x % 500_000)::INT k, (x % 41)::LONG l, (x % 97)::INT m, x v, " +
                            "timestamp_sequence('2024-01-01', 150_000) ts FROM long_sequence(2_000_000)) TIMESTAMP(ts) PARTITION BY DAY");
                    statement.execute("CREATE TABLE d1 AS (SELECT x::INT k, x w FROM long_sequence(400_000))");
                    statement.execute("CREATE TABLE d2 AS (SELECT (x % 41)::LONG l, x w2 FROM long_sequence(120))");
                    statement.execute("CREATE TABLE d3 AS (SELECT (x % 90)::INT m, x w3 FROM long_sequence(90))");
                    statement.execute("CREATE TABLE d4 AS (SELECT x::LONG l, x w4 FROM long_sequence(40))");
                    statement.execute("CREATE TABLE d5 AS (SELECT x::INT m, x w5 FROM long_sequence(96))");
                    final StringSink plan = new StringSink();
                    try (ResultSet resultSet = statement.executeQuery("EXPLAIN " + STAR)) {
                        while (resultSet.next()) {
                            plan.put(resultSet.getString(1)).put('\n');
                        }
                    }
                    TestUtils.assertContains(plan, "Hash Join workers: " + workerCount);
                    TestUtils.assertContains(plan, "stages: 3");
                    // The rows through PG wire, which runs the operator.
                    final StringSink actual = new StringSink();
                    connection.setAutoCommit(false);
                    try (PreparedStatement select = connection.prepareStatement(STAR)) {
                        select.setFetchSize(1_000);
                        try (ResultSet resultSet = select.executeQuery()) {
                            final int columnCount = resultSet.getMetaData().getColumnCount();
                            while (resultSet.next()) {
                                for (int i = 1; i <= columnCount; i++) {
                                    final String value = resultSet.getString(i);
                                    actual.put(value != null ? value : "null").put(i < columnCount ? ',' : '\n');
                                }
                            }
                        }
                    }
                    connection.setAutoCommit(true);
                    try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, workerCount).with(AllowAllSecurityContext.INSTANCE)) {
                        // The ordinary plan's rows on the same server.
                        context.setParallelHashJoinEnabled(false);
                        final String expected = rows(engine, context, STAR);
                        Assert.assertTrue(expected.length() > 1_000);
                        TestUtils.assertEquals(expected, actual);
                        context.setParallelHashJoinEnabled(true);
                        // The first stage builds in rounds, and the operator's rows equal the ordinary plan's.
                        try (RecordCursorFactory factory = engine.select(STAR, context)) {
                            final AsyncHashJoinRecordCursorFactory join = ParallelHashJoinTestUtils.find(factory);
                            Assert.assertNotNull(join);
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                Assert.assertTrue(cursor.hasNext());
                                Assert.assertTrue(join.getAtom().getStage(0).isBuiltInRounds());
                            }
                        }
                        Assert.assertEquals(expected, rows(engine, context, STAR));
                        // A stack of five joins over full-size probe frames, of 100_000 rows whatever the
                        // workers, since f's daily partitions hold 576_000 rows and the frames at least
                        // the server's 100_000, keeps the whole window.
                        try (RecordCursorFactory factory = engine.select(STACK5, context)) {
                            final AsyncHashJoinRecordCursorFactory join = ParallelHashJoinTestUtils.find(factory);
                            Assert.assertNotNull(join);
                            Assert.assertEquals(5, join.getSpec().getStageCount());
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                Assert.assertTrue(cursor.hasNext());
                                final HashJoinTupleRing ring = join.getAtom().getRing();
                                Assert.assertEquals(Math.min(dispatchLimit, queueCapacity), ring.getEntryCount());
                                Assert.assertEquals(Math.min(dispatchLimit, queueCapacity), ring.getDispatchLimit());
                                Assert.assertTrue(join.getProbeFrameCount() > 1);
                                Assert.assertEquals(100_000, join.getProbeFrameRowCount(0));
                            }
                        }
                    }
                }
            }
        });
    }

    // The query's rows, every column a LONG, as PG wire's text values render them.
    private static String rows(CairoEngine engine, SqlExecutionContextImpl context, String sql) throws Exception {
        final StringSink sink = new StringSink();
        try (RecordCursorFactory factory = engine.select(sql, context); RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertEquals(context.isParallelHashJoinEnabled(), ParallelHashJoinTestUtils.find(factory) != null);
            final int columnCount = factory.getMetadata().getColumnCount();
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                for (int i = 0; i < columnCount; i++) {
                    final long value = record.getLong(i);
                    if (value == Numbers.LONG_NULL) {
                        sink.put("null");
                    } else {
                        sink.put(value);
                    }
                    sink.put(i < columnCount - 1 ? ',' : '\n');
                }
            }
        }
        return sink.toString();
    }
}
