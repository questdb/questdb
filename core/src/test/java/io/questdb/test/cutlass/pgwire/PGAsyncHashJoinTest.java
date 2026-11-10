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

package io.questdb.test.cutlass.pgwire;

import io.questdb.Metrics;
import io.questdb.cutlass.pgwire.DefaultPGCircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Os;
import io.questdb.std.str.StringSink;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Drives the parallel hash join through PG wire with the real pgjdbc driver: named statements whose
 * bind variables sit in a LEFT join's ON residual, in the probe input's WHERE clause and in a
 * post-join filter, each executed many times with new values, in text and binary transfer, against
 * a server whose connections run the operator and one whose connections run the ordinary plan. The
 * two servers return the same rows in the same order.
 */
public class PGAsyncHashJoinTest extends BasePGTest {
    // [lo, hi]: the values each execution binds.
    private static final long[][] BINDS = {{3, 40}, {0, 90}, {7, 12}, {100, 200}, {3, 40}};
    private static final String[] QUERIES = {
            // A bind variable in the ON residual and one in the probe's WHERE clause.
            "SELECT f.ts, f.k, f.v, d.w, d.sym FROM f LEFT JOIN d ON f.k = d.k AND d.w > ? WHERE f.v < ?",
            // Both in the WHERE clause: one filters the build, the other is a post-join filter.
            "SELECT f.ts, f.k, f.v, d.w FROM f JOIN d ON f.k = d.k WHERE d.w >= ? AND f.v + d.w < ? AND f.ts IN '2024-01-01'",
            // A residual over both inputs and a bound post-join filter of a LEFT join.
            "SELECT f.k, f.v, d.w FROM f LEFT JOIN d ON f.k = d.k AND d.w + ? > f.v WHERE d.w IS NULL OR d.w < ? ORDER BY f.ts DESC",
    };
    private static final int WORKER_COUNT = 4;

    @Test
    public void testNamedStatementsWithBindVariablesInOnAndWhere() throws Exception {
        assertNamedStatements(null);
    }

    @Test
    public void testNamedStatementsWithBindVariablesOnQueryWorkers() throws Exception {
        assertNamedStatements(WorkerPoolMode.LEGACY);
    }

    private static String explain(Connection connection, String sql, long[] values) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("EXPLAIN " + sql)) {
            statement.setLong(1, values[0]);
            statement.setLong(2, values[1]);
            try (ResultSet resultSet = statement.executeQuery()) {
                final StringBuilder plan = new StringBuilder();
                while (resultSet.next()) {
                    plan.append(resultSet.getString(1)).append('\n');
                }
                return plan.toString();
            }
        }
    }

    private void assertNamedStatements(@Nullable WorkerPoolMode queryPoolMode) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE f AS (SELECT (x % 9)::INT k, x v, timestamp_sequence('2024-01-01', 1_800_000_000) ts FROM long_sequence(120)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d AS (SELECT CASE WHEN x % 10 = 0 THEN NULL ELSE (x % 7)::INT END k, x w, ('s' || (x % 3))::SYMBOL sym FROM long_sequence(40))");
            // The rows of every query and bind set, read through the ordinary plan.
            final StringSink expected = new StringSink();
            final StringSink actual = new StringSink();
            withServer(false, queryPoolMode, port -> read(port, expected, false));
            withServer(true, queryPoolMode, port -> read(port, actual, true));
            Assert.assertTrue(expected.length() > 0);
            TestUtils.assertEquals(expected, actual);
        });
    }

    // Executes every query's named statement with every bind set, twice over, in binary and text
    // transfer, and appends the rows to the sink.
    private void read(int port, StringSink sink, boolean isParallel) throws Exception {
        final StringSink rows = new StringSink();
        for (boolean isBinary : new boolean[]{true, false}) {
            for (String sql : QUERIES) {
                // A prepare threshold of 1 makes pgjdbc parse a named statement once and bind it again
                // on every later execution.
                try (
                        Connection connection = getConnection(Mode.EXTENDED, port, isBinary, 1);
                        PreparedStatement statement = connection.prepareStatement(sql)
                ) {
                    for (int pass = 0; pass < 2; pass++) {
                        for (long[] values : BINDS) {
                            statement.setLong(1, values[0]);
                            statement.setLong(2, values[1]);
                            try (ResultSet resultSet = statement.executeQuery()) {
                                rows.clear();
                                printToSink(rows, resultSet, null);
                            }
                            sink.put(sql).put(' ').put(values[0]).put(',').put(values[1]).put('\n').put(rows);
                        }
                    }
                    final String plan = explain(connection, sql, BINDS[0]);
                    Assert.assertEquals(plan, isParallel, plan.contains("Hash Join workers:"));
                }
            }
        }
    }

    /**
     * Runs the code against a PG server whose connections run the parallel hash join or not, with a
     * query pool of their own when asked; without one the owner runs every task.
     */
    private void withServer(boolean isParallel, @Nullable WorkerPoolMode queryPoolMode, ServerCode code) throws Exception {
        final Port0PGConfiguration configuration = new Port0PGConfiguration() {
            @Override
            public int getWorkerCount() {
                return 2;
            }
        };
        final TestWorkerPool queryPool = queryPoolMode == null
                ? null
                : new TestWorkerPool("query", WORKER_COUNT, Metrics.DISABLED, queryPoolMode);
        try {
            if (queryPool != null) {
                WorkerPoolUtils.setupQueryJobs(queryPool, engine, true);
                queryPool.start(LOG);
            }
            final TestWorkerPool networkPool = new TestWorkerPool(configuration);
            try (
                    PGServer server = createPGWireServer(
                            configuration,
                            engine,
                            networkPool,
                            new DefaultPGCircuitBreakerRegistry(configuration, engine.getConfiguration()),
                            () -> {
                                final SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, WORKER_COUNT);
                                context.setParallelHashJoinEnabled(isParallel);
                                return context;
                            }
                    );
                    WorkerPool ignore = networkPool
            ) {
                Assert.assertNotNull(server);
                networkPool.start(LOG);
                for (int i = 0; i < 60_000 && !server.isListening(); i++) {
                    Os.sleep(1);
                }
                code.run(server.getPort());
            }
        } finally {
            if (queryPool != null) {
                queryPool.halt();
            }
        }
    }

    @FunctionalInterface
    private interface ServerCode {
        void run(int port) throws Exception;
    }
}
