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
 * Tests for geomean(value) aggregate function which returns the geometric mean.
 * <p>
 * The geometric mean is calculated as exp(avg(ln(x))) = exp(sum(ln(x)) / count).
 * <p>
 * Test scenarios covered:
 * <p>
 * 1. Basic functionality (sequential execution):
 * <ul>
 *   <li>{@link #testGeomeanSimple()} - basic geomean with valid positive values</li>
 *   <li>{@link #testGeomeanWithGroupBy()} - geomean with GROUP BY clause</li>
 *   <li>{@link #testGeomeanAllNull()} - all values are null, result should be null</li>
 *   <li>{@link #testGeomeanWithNullValues()} - some values are null, should be ignored</li>
 *   <li>{@link #testGeomeanWithNegativeValue()} - negative value returns NaN</li>
 *   <li>{@link #testGeomeanWithZeroValue()} - zero value returns NaN</li>
 *   <li>{@link #testGeomeanSingleValue()} - single value returns that value</li>
 *   <li>{@link #testGeomeanMathematicalVerification()} - verify against known formula</li>
 * </ul>
 * <p>
 * 2. Parallel execution (tests merge logic):
 * <ul>
 *   <li>{@link #testGeomeanParallel()} - verifies parallel execution plan with 4 workers</li>
 *   <li>{@link #testGeomeanParallelWithVerification()} - parallel execution with result verification</li>
 *   <li>{@link #testGeomeanParallelChunky()} - large dataset (2M rows) parallel execution</li>
 * </ul>
 * <p>
 * 3. Parallel execution with edge cases:
 * <ul>
 *   <li>{@link #testGeomeanParallelWithNullValues()} - 50% null values, tests merge with varying counts</li>
 *   <li>{@link #testGeomeanParallelAllNullValues()} - all values null, tests merge when both have zero count</li>
 *   <li>{@link #testGeomeanParallelWithNegativeValues()} - tests merge when NaN propagates</li>
 * </ul>
 */
public class GeomeanDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testGeomeanAllNull() throws SqlException {
        execute("create table tab (value double)");

        execute("insert into tab values (null)");
        execute("insert into tab values (null)");
        execute("insert into tab values (null)");

        assertSql(
                """
                        geomean
                        null
                        """,
                "select geomean(value) from tab"
        );
    }

    @Test
    public void testGeomeanEmptyTable() throws SqlException {
        execute("create table tab (value double)");

        assertSql(
                """
                        geomean
                        null
                        """,
                "select geomean(value) from tab"
        );
    }

    @Test
    public void testGeomeanMathematicalVerification() throws SqlException {
        // Geometric mean of 2, 8 = sqrt(2 * 8) = sqrt(16) = 4
        execute("create table tab (value double)");
        execute("insert into tab values (2.0)");
        execute("insert into tab values (8.0)");

        assertSql(
                """
                        round
                        4.0
                        """,
                "select round(geomean(value), 10) from tab"
        );

        // Geometric mean of 1, 3, 9 = (1 * 3 * 9)^(1/3) = 27^(1/3) = 3
        execute("drop table tab");
        execute("create table tab (value double)");
        execute("insert into tab values (1.0)");
        execute("insert into tab values (3.0)");
        execute("insert into tab values (9.0)");

        assertSql(
                """
                        round
                        3.0
                        """,
                "select round(geomean(value), 10) from tab"
        );

        // Geometric mean of 4, 4, 4, 4 = 4
        execute("drop table tab");
        execute("create table tab (value double)");
        execute("insert into tab values (4.0)");
        execute("insert into tab values (4.0)");
        execute("insert into tab values (4.0)");
        execute("insert into tab values (4.0)");

        assertSql(
                """
                        round
                        4.0
                        """,
                "select round(geomean(value), 10) from tab"
        );
    }

    @Test
    public void testGeomeanParallel() throws Exception {
        execute("create table tab as (" +
                "select rnd_symbol('A','B','C') sym, " +
                "abs(rnd_double()) + 0.001 value " +
                "from long_sequence(10000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "select sym, geomean(value) from tab group by sym order by sym";

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
                                              values: [geomean(value)]
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
    public void testGeomeanParallelAllNullValues() throws Exception {
        // Create dataset where ALL values are null to test merge when both src and dest have zero count
        execute("create table tab as (" +
                "select " +
                "  rnd_symbol('A','B','C','D','E') sym, " +
                "  cast(null as double) value " +
                "from long_sequence(100000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "select sym, geomean(value) from tab group by sym order by sym";

                        // All results should be null since all values are null
                        TestUtils.assertSql(
                                engine,
                                sqlExecutionContext,
                                sql,
                                sink,
                                """
                                        sym\tgeomean
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
    public void testGeomeanParallelChunky() throws Exception {
        // Create large dataset with 2 million rows of positive values
        execute("create table tab as (" +
                "select " +
                "  rnd_symbol('A','B','C','D','E') sym, " +
                "  abs(rnd_double()) + 0.001 value " +
                "from long_sequence(2000000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "select sym, geomean(value) from tab group by sym order by sym";

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
                                              values: [geomean(value)]
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
    public void testGeomeanParallelWithNegativeValues() throws Exception {
        // Create dataset where some rows have negative values to test NaN propagation in merge
        execute("create table tab as (" +
                "select " +
                "  rnd_symbol('A','B','C','D','E') sym, " +
                "  case when x % 1000 = 0 then -1.0 else abs(rnd_double()) + 0.001 end value " +
                "from long_sequence(100000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "select sym, geomean(value) from tab group by sym order by sym";

                        // All results should be null (NaN displayed as null) since each group has negative values
                        TestUtils.assertSql(
                                engine,
                                sqlExecutionContext,
                                sql,
                                sink,
                                """
                                        sym\tgeomean
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
    public void testGeomeanParallelWithNullValues() throws Exception {
        // Create dataset where many rows have null values to test merge with varying counts
        // Use case() to make ~50% of values null
        execute("create table tab as (" +
                "select " +
                "  rnd_symbol('A','B','C','D','E') sym, " +
                "  case when x % 2 = 0 then null else abs(rnd_double()) + 0.001 end value " +
                "from long_sequence(2000000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "select sym, geomean(value) from tab group by sym order by sym";

                        // Run query - this exercises merge with null values
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
    public void testGeomeanParallelWithVerification() throws Exception {
        // Create deterministic test data with known positive values
        execute("create table tab as (" +
                "select " +
                "  rnd_symbol('A','B','C') sym, " +
                "  abs(rnd_double()) + 0.001 value " +
                "from long_sequence(10000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        String sql = "select sym, geomean(value) from tab group by sym order by sym";

                        // Run parallel query and verify it produces consistent results
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
    public void testGeomeanSimple() throws SqlException {
        execute("create table tab (value double)");

        execute("insert into tab values (1.0)");
        execute("insert into tab values (2.0)");
        execute("insert into tab values (4.0)");

        // Geometric mean of 1, 2, 4 = (1 * 2 * 4)^(1/3) = 8^(1/3) = 2
        assertSql(
                """
                        geomean
                        2.0
                        """,
                "select geomean(value) from tab"
        );
    }

    @Test
    public void testGeomeanSingleValue() throws SqlException {
        execute("create table tab (value double)");
        execute("insert into tab values (5.0)");

        // Geometric mean of single value is that value
        assertSql(
                """
                        round
                        5.0
                        """,
                "select round(geomean(value), 10) from tab"
        );
    }

    @Test
    public void testGeomeanWithGroupBy() throws SqlException {
        execute("create table tab (sym symbol, value double)");

        // Group A: geomean(2, 8) = sqrt(16) = 4
        execute("insert into tab values ('A', 2.0)");
        execute("insert into tab values ('A', 8.0)");

        // Group B: geomean(1, 3, 9) = (27)^(1/3) = 3
        execute("insert into tab values ('B', 1.0)");
        execute("insert into tab values ('B', 3.0)");
        execute("insert into tab values ('B', 9.0)");

        assertSql(
                """
                        sym\tround
                        A\t4.0
                        B\t3.0
                        """,
                "select sym, round(geomean(value), 10) from tab order by sym"
        );
    }

    @Test
    public void testGeomeanWithMixedValidAndInvalid() throws SqlException {
        execute("create table tab (sym symbol, value double)");

        // Group A: has negative value -> null (NaN displayed as null)
        execute("insert into tab values ('A', 2.0)");
        execute("insert into tab values ('A', -1.0)");
        execute("insert into tab values ('A', 8.0)");

        // Group B: all positive -> valid result
        execute("insert into tab values ('B', 1.0)");
        execute("insert into tab values ('B', 3.0)");
        execute("insert into tab values ('B', 9.0)");

        // Group C: has zero -> null (NaN displayed as null)
        execute("insert into tab values ('C', 2.0)");
        execute("insert into tab values ('C', 0.0)");
        execute("insert into tab values ('C', 8.0)");

        assertSql(
                """
                        sym\tround
                        A\tnull
                        B\t3.0
                        C\tnull
                        """,
                "select sym, round(geomean(value), 10) from tab order by sym"
        );
    }

    @Test
    public void testGeomeanWithNegativeValue() throws SqlException {
        execute("create table tab (value double)");

        execute("insert into tab values (2.0)");
        execute("insert into tab values (-1.0)");
        execute("insert into tab values (8.0)");

        // Negative value -> null (NaN displayed as null)
        assertSql(
                """
                        geomean
                        null
                        """,
                "select geomean(value) from tab"
        );
    }

    @Test
    public void testGeomeanWithNullValues() throws SqlException {
        execute("create table tab (value double)");

        execute("insert into tab values (null)");
        execute("insert into tab values (2.0)");
        execute("insert into tab values (8.0)");
        execute("insert into tab values (null)");

        // Null values are ignored, geomean(2, 8) = 4
        assertSql(
                """
                        geomean
                        4.0
                        """,
                "select geomean(value) from tab"
        );
    }

    @Test
    public void testGeomeanWithZeroValue() throws SqlException {
        execute("create table tab (value double)");

        execute("insert into tab values (2.0)");
        execute("insert into tab values (0.0)");
        execute("insert into tab values (8.0)");

        // Zero value -> null (NaN displayed as null)
        assertSql(
                """
                        geomean
                        null
                        """,
                "select geomean(value) from tab"
        );
    }

    @Test
    public void testGeomeanAgainstExpAvgLnFormula() throws SqlException {
        // Verify that geomean(x) = exp(avg(ln(x))) for positive values
        execute("create table tab as (" +
                "select " +
                "  rnd_symbol('A','B','C') sym, " +
                "  abs(rnd_double()) + 0.001 value " +
                "from long_sequence(1000))");

        assertSql(
                """
                        sym\tis_equal
                        A\ttrue
                        B\ttrue
                        C\ttrue
                        """,
                """
                        select
                            sym,
                            abs(geomean(value) - exp(avg(ln(value)))) < 0.0000001 as is_equal
                        from tab
                        group by sym
                        order by sym
                        """
        );
    }

    @Test
    public void testGeomeanConstantArgument() throws SqlException {
        // When the argument is a constant, the factory returns DoubleConstant directly
        // geomean(c) = c for any positive constant c
        execute("create table tab (x int)");
        execute("insert into tab values (1), (2), (3)");

        // Verify the result is correct: geomean(5.0) = 5.0
        assertSql(
                """
                        geomean
                        5.0
                        """,
                "select geomean(5.0) from tab"
        );

        // Verify with negative constant - should return null (NaN)
        assertSql(
                """
                        geomean
                        null
                        """,
                "select geomean(-5.0) from tab"
        );

        // Verify with zero constant - should return null (NaN)
        assertSql(
                """
                        geomean
                        null
                        """,
                "select geomean(0.0) from tab"
        );

        // Verify with null constant
        assertSql(
                """
                        geomean
                        null
                        """,
                "select geomean(null) from tab"
        );
    }
}
