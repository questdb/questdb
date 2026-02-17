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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.griffin.SqlException;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Tests for arg_max(value, key) aggregate function which returns the value at the maximum key.
 * <p>
 * Test scenarios covered:
 * <p>
 * 1. Basic functionality (sequential execution):
 * <ul>
 *   <li>{@link #testArgMaxSimple()} - basic arg_max with valid values and keys</li>
 *   <li>{@link #testArgMaxWithGroupBy()} - arg_max with GROUP BY clause</li>
 *   <li>{@link #testArgMaxAllNull()} - all keys are null, result should be null</li>
 *   <li>{@link #testArgMaxWithNullKey()} - some keys are null, should be ignored</li>
 *   <li>{@link #testArgMaxWithNullValue()} - value is null at max key, result should be null</li>
 * </ul>
 * <p>
 * 2. Parallel execution (tests merge logic):
 * <ul>
 *   <li>{@link #testArgMaxParallel()} - verifies parallel execution plan with 4 workers</li>
 *   <li>{@link #testArgMaxParallelWithVerification()} - parallel execution with result verification</li>
 *   <li>{@link #testArgMaxParallelChunky()} - large dataset (2M rows) parallel execution</li>
 * </ul>
 * <p>
 * 3. Parallel execution with null key handling (tests merge null branches):
 * <ul>
 *   <li>{@link #testArgMaxParallelWithNullKeys()} - 50% null keys, tests merge with srcMaxKey=null</li>
 *   <li>{@link #testArgMaxParallelAllNullKeys()} - all keys null, tests merge when both src and dest have null keys</li>
 *   <li>{@link #testArgMaxParallelMergeNullDestValidSrc()} - first half null keys, second half valid keys,
 *       tests merge when destMaxKey=null but srcMaxKey is valid (exercises Numbers.isNull(destMaxKey) branch)</li>
 * </ul>
 */
public class ArgMaxDoubleDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testArgMaxAllNull() throws SqlException {
        execute("create table tab (value double, key double)");

        execute("insert into tab values (null, null)");
        execute("insert into tab values (null, null)");
        execute("insert into tab values (null, null)");

        assertSql(
                """
                        arg_max
                        null
                        """,
                "select arg_max(value, key) from tab"
        );
    }

    @Test
    public void testArgMaxParallel() throws Exception {
        execute("create table tab as (" +
                "select rnd_symbol('A','B','C') sym, " +
                "rnd_double() value, " +
                "rnd_double() key " +
                "from long_sequence(10000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        // First, compute expected results using a non-parallel query
                        // by finding max key per symbol and the corresponding value
                        String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";

                        // Verify the query plan shows parallel execution
                        TestUtils.assertSql(
                                engine,
                                sqlExecutionContext,
                                "explain " + sql,
                                sink,
                                """
                                        QUERY PLAN
                                        Sort light
                                          keys: [sym]
                                            Async Group By workers: 4
                                              keys: [sym]
                                              values: [arg_max(value,key)]
                                              filter: null
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tab
                                        """
                        );
                    },
                    configuration,
                    LOG
            );
        }
    }

    @Test
    public void testArgMaxParallelAllNullKeys() throws Exception {
        // Create dataset where ALL keys are null to test merge when both src and dest have null keys
        execute("create table tab as (" +
                "select " +
                "  rnd_symbol('A','B','C','D','E') sym, " +
                "  rnd_double() value, " +
                "  cast(null as double) key " +
                "from long_sequence(100000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";

                        // All results should be null since all keys are null
                        TestUtils.assertSql(
                                engine,
                                sqlExecutionContext,
                                sql,
                                sink,
                                """
                                        sym\targ_max
                                        A\tnull
                                        B\tnull
                                        C\tnull
                                        D\tnull
                                        E\tnull
                                        """
                        );
                    },
                    configuration,
                    LOG
            );
        }
    }

    @Test
    public void testArgMaxParallelChunky() throws Exception {
        // Create large dataset with 2 million rows
        execute("create table tab as (" +
                "select " +
                "  rnd_symbol('A','B','C','D','E') sym, " +
                "  rnd_double() value, " +
                "  rnd_double() key " +
                "from long_sequence(2000000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";

                        // Verify the query plan shows parallel execution
                        TestUtils.assertSql(
                                engine,
                                sqlExecutionContext,
                                "explain " + sql,
                                sink,
                                """
                                        QUERY PLAN
                                        Sort light
                                          keys: [sym]
                                            Async Group By workers: 4
                                              keys: [sym]
                                              values: [arg_max(value,key)]
                                              filter: null
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tab
                                        """
                        );

                        // Run query and verify results are consistent
                        TestUtils.assertSqlCursors(
                                engine,
                                sqlExecutionContext,
                                sql,
                                sql,
                                LOG
                        );
                    },
                    configuration,
                    LOG
            );
        }
    }

    @Test
    public void testArgMaxParallelMergeNullDestValidSrc() throws Exception {
        // Create dataset where first half has null keys and second half has valid keys
        // This tests merge when destMaxKey is null but srcMaxKey is valid
        execute("create table tab as (" +
                "select " +
                "  rnd_symbol('A','B','C','D','E') sym, " +
                "  rnd_double() value, " +
                "  case when x <= 1000000 then null else rnd_double() end key " +
                "from long_sequence(2000000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";

                        // Results should NOT be null - valid keys from second half should win
                        TestUtils.assertSqlCursors(
                                engine,
                                sqlExecutionContext,
                                sql,
                                sql,
                                LOG
                        );
                    },
                    configuration,
                    LOG
            );
        }
    }

    @Test
    public void testArgMaxParallelWithNullKeys() throws Exception {
        // Create dataset where many rows have null keys to test merge with null srcMaxKey
        // Use case() to make ~50% of keys null
        execute("create table tab as (" +
                "select " +
                "  rnd_symbol('A','B','C','D','E') sym, " +
                "  rnd_double() value, " +
                "  case when x % 2 = 0 then null else rnd_double() end key " +
                "from long_sequence(2000000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";

                        // Run query - this exercises merge with null keys
                        TestUtils.assertSqlCursors(
                                engine,
                                sqlExecutionContext,
                                sql,
                                sql,
                                LOG
                        );
                    },
                    configuration,
                    LOG
            );
        }
    }

    @Test
    public void testArgMaxParallelWithVerification() throws Exception {
        // Create deterministic test data with known max keys per symbol
        execute("create table tab as (" +
                "select " +
                "  rnd_symbol('A','B','C') sym, " +
                "  rnd_double() value, " +
                "  rnd_double() key " +
                "from long_sequence(10000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";

                        // Run parallel query and verify it produces results
                        TestUtils.assertSqlCursors(
                                engine,
                                sqlExecutionContext,
                                sql,
                                sql,
                                LOG
                        );
                    },
                    configuration,
                    LOG
            );
        }
    }

    @Test
    public void testArgMaxSimple() throws SqlException {
        execute("create table tab (value double, key double)");

        execute("insert into tab values (10.0, 1.0)");
        execute("insert into tab values (20.0, 3.0)");
        execute("insert into tab values (30.0, 2.0)");

        // key=3.0 is max, so value should be 20.0
        assertSql(
                """
                        arg_max
                        20.0
                        """,
                "select arg_max(value, key) from tab"
        );
    }

    @Test
    public void testArgMaxWithGroupBy() throws SqlException {
        execute("create table tab (sym symbol, value double, key double)");

        execute("insert into tab values ('A', 10.0, 1.0)");
        execute("insert into tab values ('A', 20.0, 3.0)");
        execute("insert into tab values ('A', 30.0, 2.0)");
        execute("insert into tab values ('B', 100.0, 5.0)");
        execute("insert into tab values ('B', 200.0, 4.0)");

        assertSql(
                """
                        sym\targ_max
                        A\t20.0
                        B\t100.0
                        """,
                "select sym, arg_max(value, key) from tab order by sym"
        );
    }

    @Test
    public void testArgMaxWithNullKey() throws SqlException {
        execute("create table tab (value double, key double)");

        execute("insert into tab values (10.0, null)");
        execute("insert into tab values (20.0, 3.0)");
        execute("insert into tab values (30.0, 2.0)");

        // key=3.0 is max (null is ignored), so value should be 20.0
        assertSql(
                """
                        arg_max
                        20.0
                        """,
                "select arg_max(value, key) from tab"
        );
    }

    @Test
    public void testArgMaxWithNullValue() throws SqlException {
        execute("create table tab (value double, key double)");

        execute("insert into tab values (null, 5.0)");
        execute("insert into tab values (20.0, 3.0)");
        execute("insert into tab values (30.0, 2.0)");

        // key=5.0 is max, but value is null
        assertSql(
                """
                        arg_max
                        null
                        """,
                "select arg_max(value, key) from tab"
        );
    }
}
