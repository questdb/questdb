/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;

import static org.junit.Assert.fail;

public class ParallelGroupByTest extends AbstractCairoTest {

    @Test
    public void testParallelMultipleKeyedGroupBy() throws Exception {
        testParallelMultipleKeyedGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab",
                "key1\tkey2\tavg\tsum\n" +
                        "k1\tk1\t48.5\t735.0\n" +
                        "k2\tk2\t49.5\t745.0\n" +
                        "k3\tk3\t50.5\t755.0\n" +
                        "k4\tk4\t51.5\t765.0\n" +
                        "k0\tk0\t52.5\t775.0\n"
        );
    }

    @Test
    public void testParallelMultipleKeyedGroupBySubQuery() throws Exception {
        testParallelMultipleKeyedGroupBy(
                "SELECT key1, key2, avg + sum from (" +
                        "SELECT key1, key2, avg(value), sum(colTop) FROM tab" +
                        ")",
                "key1\tkey2\tcolumn\n" +
                        "k1\tk1\t783.5\n" +
                        "k2\tk2\t794.5\n" +
                        "k3\tk3\t805.5\n" +
                        "k4\tk4\t816.5\n" +
                        "k0\tk0\t827.5\n"
        );
    }

    // todo: this query does not use AsyncGroupByRecordCursorFactory
    //  AsyncFilteredRecordCursorFactory should support PageFrameCursor
    @Test
    public void testParallelMultipleKeyedGroupByWithFilter() throws Exception {
        testParallelMultipleKeyedGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab WHERE value < 80",
                "key1\tkey2\tavg\tsum\n" +
                        "k1\tk1\t38.5\t381.0\n" +
                        "k2\tk2\t39.5\t387.0\n" +
                        "k3\tk3\t40.5\t393.0\n" +
                        "k4\tk4\t41.5\t399.0\n" +
                        "k0\tk0\t40.0\t325.0\n"
        );
    }

    @Test
    public void testParallelMultipleKeyedGroupByWithLimit() throws Exception {
        testParallelMultipleKeyedGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab LIMIT 3",
                "key1\tkey2\tavg\tsum\n" +
                        "k1\tk1\t48.5\t735.0\n" +
                        "k2\tk2\t49.5\t745.0\n" +
                        "k3\tk3\t50.5\t755.0\n",
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab LIMIT -3",
                "key1\tkey2\tavg\tsum\n" +
                        "k3\tk3\t50.5\t755.0\n" +
                        "k4\tk4\t51.5\t765.0\n" +
                        "k0\tk0\t52.5\t775.0\n"
        );
    }

    @Test
    public void testParallelSingleKeyedGroupBy() throws Exception {
        testParallelSingleKeyedGroupBy(
                "SELECT key, avg(value), sum(colTop) FROM tab",
                "key\tavg\tsum\n" +
                        "k1\t48.5\t735.0\n" +
                        "k2\t49.5\t745.0\n" +
                        "k3\t50.5\t755.0\n" +
                        "k4\t51.5\t765.0\n" +
                        "k0\t52.5\t775.0\n"
        );
    }

    @Test
    public void testParallelSingleKeyedGroupByConcurrent() throws Exception {
        final int numOfThreads = 20;
        final int numOfIterations = 50;
        final String query = "SELECT key, avg + sum from (" +
                "SELECT key, avg(value), sum(colTop) FROM tab" +
                ")";
        final String expected = "key\tcolumn\n" +
                "k1\t783.5\n" +
                "k2\t794.5\n" +
                "k3\t805.5\n" +
                "k4\t816.5\n" +
                "k0\t827.5\n";

        final ConcurrentHashMap<Integer, Throwable> errors = new ConcurrentHashMap<>();
        final WorkerPool pool = new WorkerPool((() -> 4));
        TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    ddl(compiler, "CREATE TABLE tab (\n" +
                            "  ts TIMESTAMP," +
                            "  key STRING," +
                            "  value DOUBLE ) timestamp (ts) PARTITION BY DAY", sqlExecutionContext);
                    insert(compiler, "insert into tab select (x*8640000000)::timestamp, 'k' || (x%5), x from long_sequence(50)", sqlExecutionContext);
                    ddl(compiler, "ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                    insert(compiler, "insert into tab select ((50 + x)*8640000000)::timestamp, 'k' || ((50 + x)%5), 50 + x, 50 + x from long_sequence(50)", sqlExecutionContext);

                    final CyclicBarrier barrier = new CyclicBarrier(numOfThreads);
                    final SOCountDownLatch haltLatch = new SOCountDownLatch(numOfThreads);

                    for (int i = 0; i < numOfThreads; i++) {
                        final int threadId = i;
                        new Thread(() -> {
                            final StringSink sink = new StringSink();
                            TestUtils.await(barrier);
                            try {
                                for (int j = 0; j < numOfIterations; j++) {
                                    assertQueries(engine, sqlExecutionContext, sink, query, expected);
                                }
                            } catch (Throwable e) {
                                e.printStackTrace();
                                errors.put(threadId, e);
                            } finally {
                                haltLatch.countDown();
                            }
                        }).start();
                    }
                    haltLatch.await();
                },
                configuration,
                LOG
        );

        if (!errors.isEmpty()) {
            for (Map.Entry<Integer, Throwable> entry : errors.entrySet()) {
                LOG.error().$("Error in thread [id=").$(entry.getKey()).$("] ").$(entry.getValue()).$();
            }
            fail("Error in threads");
        }
    }

    @Test
    public void testParallelSingleKeyedGroupBySubQuery() throws Exception {
        testParallelSingleKeyedGroupBy(
                "SELECT key, avg + sum from (" +
                        "SELECT key, avg(value), sum(colTop) FROM tab" +
                        ")",
                "key\tcolumn\n" +
                        "k1\t783.5\n" +
                        "k2\t794.5\n" +
                        "k3\t805.5\n" +
                        "k4\t816.5\n" +
                        "k0\t827.5\n"
        );
    }

    // todo: this query does not use AsyncGroupByRecordCursorFactory
    //  AsyncFilteredRecordCursorFactory should support PageFrameCursor
    @Test
    public void testParallelSingleKeyedGroupByWithFilter() throws Exception {
        testParallelSingleKeyedGroupBy(
                "SELECT key, avg(value), sum(colTop) FROM tab WHERE value < 80",
                "key\tavg\tsum\n" +
                        "k1\t38.5\t381.0\n" +
                        "k2\t39.5\t387.0\n" +
                        "k3\t40.5\t393.0\n" +
                        "k4\t41.5\t399.0\n" +
                        "k0\t40.0\t325.0\n"
        );
    }

    @Test
    public void testParallelSingleKeyedGroupByWithLimit() throws Exception {
        testParallelSingleKeyedGroupBy(
                "SELECT key, avg(value), sum(colTop) FROM tab LIMIT 3",
                "key\tavg\tsum\n" +
                        "k1\t48.5\t735.0\n" +
                        "k2\t49.5\t745.0\n" +
                        "k3\t50.5\t755.0\n",
                "SELECT key, avg(value), sum(colTop) FROM tab LIMIT -3",
                "key\tavg\tsum\n" +
                        "k3\t50.5\t755.0\n" +
                        "k4\t51.5\t765.0\n" +
                        "k0\t52.5\t775.0\n"
        );
    }

    private static void assertQueries(CairoEngine engine, SqlExecutionContext sqlExecutionContext, String... queriesAndExpectedResults) throws SqlException {
        assertQueries(engine, sqlExecutionContext, sink, queriesAndExpectedResults);
    }

    private static void assertQueries(CairoEngine engine, SqlExecutionContext sqlExecutionContext, StringSink sink, String... queriesAndExpectedResults) throws SqlException {
        for (int i = 0, n = queriesAndExpectedResults.length; i < n; i += 2) {
            final String query = queriesAndExpectedResults[i];
            final String expected = queriesAndExpectedResults[i + 1];
            TestUtils.assertSql(
                    engine,
                    sqlExecutionContext,
                    query,
                    sink,
                    expected
            );
        }
    }

    private void testParallelMultipleKeyedGroupBy(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                        ddl(compiler, "CREATE TABLE tab (\n" +
                                "  ts TIMESTAMP," +
                                "  key1 SYMBOL," +
                                "  key2 SYMBOL," +
                                "  value DOUBLE ) timestamp (ts) PARTITION BY DAY", sqlExecutionContext);
                        insert(compiler, "insert into tab select (x*8640000000)::timestamp, 'k' || (x%5), 'k' || (x%5), x from long_sequence(50)", sqlExecutionContext);
                        ddl(compiler, "ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                        insert(compiler, "insert into tab select ((50 + x)*8640000000)::timestamp, 'k' || ((50 + x)%5), 'k' || ((50 + x)%5), 50 + x, 50 + x from long_sequence(50)", sqlExecutionContext);
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelSingleKeyedGroupBy(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                        ddl(compiler, "CREATE TABLE tab (\n" +
                                "  ts TIMESTAMP," +
                                "  key STRING," +
                                "  value DOUBLE ) timestamp (ts) PARTITION BY DAY", sqlExecutionContext);
                        insert(compiler, "insert into tab select (x*8640000000)::timestamp, 'k' || (x%5), x from long_sequence(50)", sqlExecutionContext);
                        ddl(compiler, "ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                        insert(compiler, "insert into tab select ((50 + x)*8640000000)::timestamp, 'k' || ((50 + x)%5), 50 + x, 50 + x from long_sequence(50)", sqlExecutionContext);
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }
}
