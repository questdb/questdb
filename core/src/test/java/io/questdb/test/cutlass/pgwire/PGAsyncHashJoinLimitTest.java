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

/**
 * Drives the parallel hash join under a bound LIMIT through PG wire with the real pgjdbc driver:
 * named statements whose LIMIT, and a value of the join's ON residual, are bind variables, executed
 * many times with new values, positive, larger than the table, zero and negative, in text and binary
 * transfer, against a server whose connections run the operator and one whose connections run the
 * ordinary plan. Each execution of the cached statement reads its own LIMIT, and the two servers
 * return the same rows in the same order.
 */
public class PGAsyncHashJoinLimitTest extends BasePGTest {
    // [residual value, LIMIT]: the values each execution binds.
    private static final long[][] BINDS = {{3, 2}, {0, 500}, {7, -5}, {100, 0}, {3, 17}, {0, 1}};
    private static final String[] QUERIES = {
            "SELECT f.ts, f.k, f.v, d.w FROM f LEFT JOIN d ON f.k = d.k AND d.w > ? LIMIT ?",
            "SELECT f.k, f.v, d.w, e.w FROM f JOIN d ON f.k = d.k LEFT JOIN d e ON f.k = e.k AND e.w > f.v + ? LIMIT ?",
            "SELECT f.ts, f.k, f.v, d.w FROM f JOIN d ON f.k = d.k WHERE d.w > ? ORDER BY ts DESC LIMIT ?",
    };
    private static final int WORKER_COUNT = 4;

    @Test
    public void testNamedStatementsWithBoundLimit() throws Exception {
        assertNamedStatements(null);
    }

    @Test
    public void testNamedStatementsWithBoundLimitOnQueryWorkers() throws Exception {
        assertNamedStatements(WorkerPoolMode.LEGACY);
    }

    private void assertNamedStatements(@Nullable WorkerPoolMode queryPoolMode) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE f AS (SELECT (x % 9)::INT k, x v, timestamp_sequence('2024-01-01', 1_800_000_000) ts FROM long_sequence(120)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d AS (SELECT CASE WHEN x % 10 = 0 THEN NULL ELSE (x % 7)::INT END k, x w FROM long_sequence(40))");
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
                    try (PreparedStatement explain = connection.prepareStatement("EXPLAIN " + sql)) {
                        explain.setLong(1, BINDS[0][0]);
                        explain.setLong(2, BINDS[0][1]);
                        final StringBuilder plan = new StringBuilder();
                        try (ResultSet resultSet = explain.executeQuery()) {
                            while (resultSet.next()) {
                                plan.append(resultSet.getString(1)).append('\n');
                            }
                        }
                        Assert.assertEquals(plan.toString(), isParallel, plan.toString().contains("Hash Join workers:"));
                    }
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
                                // Frames of four rows, so that a small LIMIT narrows the first window.
                                context.changePageFrameSizes(4, 4);
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
