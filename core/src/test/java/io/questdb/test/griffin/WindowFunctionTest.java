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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Tests for window functions with actual data execution.
 * These tests verify that window functions work correctly end-to-end,
 * including cases where window functions are nested inside operations.
 */
public class WindowFunctionTest extends AbstractCairoTest {

    @Test
    public void testWindowFunctionWithCast() throws Exception {
        // Test window function with :: cast operator
        assertQuery(
                """
                        rn
                        1
                        2
                        3
                        """,
                "SELECT row_number() OVER (ORDER BY ts)::string AS rn FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,  // window functions don't support random access
                true
        );
    }

    @Test
    public void testWindowFunctionWithCastInParentheses() throws Exception {
        // Test window function with cast and outer parentheses
        assertQuery(
                """
                        rn
                        1
                        2
                        3
                        """,
                "SELECT (row_number() OVER (ORDER BY ts))::string AS rn FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testTwoWindowFunctionsInArithmetic() throws Exception {
        // Test arithmetic between two window functions: sum() OVER - lag() OVER
        assertQuery(
                """
                        id_diff
                        null
                        2.0
                        4.0
                        7.0
                        11.0
                        """,
                "SELECT sum(id) OVER (ORDER BY ts) - lag(id) OVER (ORDER BY ts) AS id_diff FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionArithmeticInsideFunction() throws Exception {
        // Test two window functions in arithmetic expression inside abs()
        // This is the original bug case
        assertQuery(
                """
                        result
                        0.0
                        0.0
                        0.0
                        """,
                "SELECT abs(sum(x) OVER () - sum(x) OVER ()) AS result FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x, timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testWindowFunctionAsArgumentToCast() throws Exception {
        // Test window function as direct argument to cast() function
        assertQuery(
                """
                        result
                        1
                        2
                        3
                        """,
                "SELECT cast(row_number() OVER (ORDER BY ts) AS int) AS result FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionAsArgumentToFunction() throws Exception {
        // Test window function as direct argument to abs() function
        assertQuery(
                """
                        result
                        1
                        2
                        3
                        """,
                "SELECT abs(row_number() OVER (ORDER BY ts)) AS result FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionWithPartitionBy() throws Exception {
        // Test window function with PARTITION BY and cast
        assertQuery(
                """
                        sym\trn
                        A\t1
                        A\t2
                        B\t1
                        B\t2
                        """,
                "SELECT sym, row_number() OVER (PARTITION BY sym ORDER BY ts)::string AS rn FROM x ORDER BY sym, ts",
                "CREATE TABLE x AS (" +
                        "SELECT " +
                        "  CASE WHEN x <= 2 THEN 'A' ELSE 'B' END AS sym, " +
                        "  timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(4)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testWindowFunctionLagMinusLead() throws Exception {
        // Test lag() - lead() window function arithmetic
        assertQuery(
                """
                        diff
                        null
                        -2
                        -2
                        null
                        """,
                "SELECT lag(id) OVER (ORDER BY ts) - lead(id) OVER (ORDER BY ts) AS diff FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(4)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testWindowFunctionMultipleInSelect() throws Exception {
        // Test multiple window functions in select with different operations
        assertQuery(
                """
                        id\trunning_sum\tprev_id\tdiff
                        1\t1.0\tnull\tnull
                        2\t3.0\t1\t2.0
                        3\t6.0\t2\t4.0
                        4\t10.0\t3\t7.0
                        5\t15.0\t4\t11.0
                        """,
                "SELECT " +
                        "  id, " +
                        "  sum(id) OVER (ORDER BY ts) AS running_sum, " +
                        "  lag(id) OVER (ORDER BY ts) AS prev_id, " +
                        "  sum(id) OVER (ORDER BY ts) - lag(id) OVER (ORDER BY ts) AS diff " +
                        "FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionCastToInt() throws Exception {
        // Test window function cast to int
        assertQuery(
                """
                        rn
                        1
                        2
                        3
                        """,
                "SELECT row_number() OVER (ORDER BY ts)::int AS rn FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionWithNullHandling() throws Exception {
        // Test window functions with null values in arithmetic
        assertQuery(
                """
                        id\tprev\tdiff
                        1\tnull\tnull
                        null\t1\tnull
                        3\tnull\tnull
                        4\t3\t1
                        """,
                "SELECT id, lag(id) OVER (ORDER BY ts) AS prev, id - lag(id) OVER (ORDER BY ts) AS diff FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT " +
                        "  CASE WHEN x = 2 THEN NULL ELSE x END AS id, " +
                        "  timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(4)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionInCaseExpression() throws Exception {
        // Test window function directly inside CASE WHEN condition
        assertQuery(
                """
                        category
                        first
                        middle
                        last
                        """,
                "SELECT CASE " +
                        "  WHEN row_number() OVER (ORDER BY ts) = 1 THEN 'first' " +
                        "  WHEN row_number() OVER (ORDER BY ts) = 3 THEN 'last' " +
                        "  ELSE 'middle' " +
                        "END AS category " +
                        "FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionInCaseDifferentOverSpecs() throws Exception {
        // Window functions with different OVER specs in THEN vs ELSE branches
        assertQuery(
                """
                        id\tresult
                        1\t1.0
                        2\t3.0
                        3\t6.0
                        4\t4.0
                        5\t5.0
                        """,
                "SELECT id, CASE " +
                        "  WHEN id <= 3 THEN sum(id) OVER (ORDER BY ts) " +
                        "  ELSE sum(id) OVER (PARTITION BY id ORDER BY ts) " +
                        "END AS result " +
                        "FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionWithArithmeticInCaseBranches() throws Exception {
        // Window function with arithmetic operations in different CASE branches
        assertQuery(
                """
                        id\tresult
                        1\t2
                        2\t3
                        3\t4
                        4\t3
                        5\t4
                        """,
                "SELECT id, CASE " +
                        "  WHEN id <= 3 THEN row_number() OVER (ORDER BY ts) + 1 " +
                        "  ELSE row_number() OVER (ORDER BY ts) - 1 " +
                        "END AS result " +
                        "FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionNestedParenthesesWithCast() throws Exception {
        // Deeply nested parentheses with cast
        assertQuery(
                """
                        rn
                        1
                        2
                        3
                        """,
                "SELECT (((row_number() OVER (ORDER BY ts))))::int AS rn FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionInCaseWithCast() throws Exception {
        // Window function with cast inside CASE expression
        assertQuery(
                """
                        id\tresult
                        1\t1
                        2\t2
                        3\t3
                        4\tN/A
                        5\tN/A
                        """,
                "SELECT id, CASE " +
                        "  WHEN id <= 3 THEN row_number() OVER (ORDER BY ts)::string " +
                        "  ELSE 'N/A' " +
                        "END AS result " +
                        "FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionInCaseThenAndElse() throws Exception {
        // Window functions in both THEN and ELSE with different functions
        assertQuery(
                """
                        id\tresult
                        1\t1
                        2\t2
                        3\t3
                        4\t3
                        5\t4
                        """,
                "SELECT id, CASE " +
                        "  WHEN id <= 3 THEN row_number() OVER (ORDER BY ts) " +
                        "  ELSE lag(id) OVER (ORDER BY ts) " +
                        "END AS result " +
                        "FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionWithLimit() throws Exception {
        // Test window function with LIMIT clause
        assertQuery(
                """
                        rn
                        1
                        2
                        """,
                "SELECT row_number() OVER (ORDER BY ts) AS rn FROM x LIMIT 2",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testTwoWindowColumns() throws Exception {
        // Test two window functions with different specifications
        assertQuery(
                """
                        id\trn\ttotal
                        1\t1\t15.0
                        2\t2\t15.0
                        3\t3\t15.0
                        4\t4\t15.0
                        5\t5\t15.0
                        """,
                "SELECT id, " +
                        "row_number() OVER (ORDER BY ts) AS rn, " +
                        "sum(id) OVER () AS total " +
                        "FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testWindowOrderByDesc() throws Exception {
        // Test window function with ORDER BY DESC
        assertQuery(
                """
                        id\trn
                        1\t5
                        2\t4
                        3\t3
                        4\t2
                        5\t1
                        """,
                "SELECT id, row_number() OVER (ORDER BY ts DESC) AS rn FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testWindowMultiplePartitionBy() throws Exception {
        // Test window function with multiple PARTITION BY columns
        assertQuery(
                """
                        a\tb\trn
                        1\tX\t1
                        1\tX\t2
                        1\tY\t1
                        2\tX\t1
                        2\tY\t1
                        """,
                "SELECT a, b, row_number() OVER (PARTITION BY a, b ORDER BY ts) AS rn FROM x ORDER BY a, b, ts",
                "CREATE TABLE x AS (" +
                        "SELECT " +
                        "  CASE WHEN x <= 3 THEN 1 ELSE 2 END AS a, " +
                        "  CASE WHEN x IN (1, 2, 4) THEN 'X' ELSE 'Y' END AS b, " +
                        "  timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testWindowFrameRowsBetween() throws Exception {
        // Test window function with ROWS BETWEEN frame specification
        // Note: QuestDB only supports PRECEDING and CURRENT ROW for frame end
        assertQuery(
                """
                        id\tmoving_sum
                        1\t1.0
                        2\t3.0
                        3\t5.0
                        4\t7.0
                        5\t9.0
                        """,
                "SELECT id, sum(id) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS moving_sum FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowAvg() throws Exception {
        // Test avg() window function
        assertQuery(
                """
                        id\tavg_val
                        1\t1.0
                        2\t1.5
                        3\t2.0
                        4\t2.5
                        5\t3.0
                        """,
                "SELECT id, avg(id) OVER (ORDER BY ts) AS avg_val FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFirstValue() throws Exception {
        // Test first_value() window function
        assertQuery(
                """
                        id\tfirst_val
                        1\t1
                        2\t1
                        3\t1
                        4\t1
                        5\t1
                        """,
                "SELECT id, first_value(id) OVER (ORDER BY ts) AS first_val FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowRank() throws Exception {
        // Test rank() window function with ties
        assertQuery(
                """
                        val\trnk
                        1\t1
                        1\t1
                        2\t3
                        3\t4
                        3\t4
                        """,
                "SELECT val, rank() OVER (ORDER BY val) AS rnk FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT " +
                        "  CASE WHEN x IN (1, 2) THEN 1 WHEN x = 3 THEN 2 ELSE 3 END AS val, " +
                        "  timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testWindowDenseRank() throws Exception {
        // Test dense_rank() window function with ties
        assertQuery(
                """
                        val\tdrnk
                        1\t1
                        1\t1
                        2\t2
                        3\t3
                        3\t3
                        """,
                "SELECT val, dense_rank() OVER (ORDER BY val) AS drnk FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT " +
                        "  CASE WHEN x IN (1, 2) THEN 1 WHEN x = 3 THEN 2 ELSE 3 END AS val, " +
                        "  timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testWindowCount() throws Exception {
        // Test count() window function
        assertQuery(
                """
                        id\tcnt
                        1\t1
                        2\t2
                        3\t3
                        4\t4
                        5\t5
                        """,
                "SELECT id, count(*) OVER (ORDER BY ts) AS cnt FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowCumulativeSum() throws Exception {
        // Test cumulative sum using ROWS UNBOUNDED PRECEDING
        assertQuery(
                """
                        id\tcum_sum
                        1\t1.0
                        2\t3.0
                        3\t6.0
                        4\t10.0
                        5\t15.0
                        """,
                "SELECT id, sum(id) OVER (ORDER BY ts ROWS UNBOUNDED PRECEDING) AS cum_sum FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT x AS id, timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }

    @Test
    public void testWindowFunctionArithmeticWithOrderBy() throws Exception {
        // Test window function with ORDER BY and arithmetic: row_number() + 1
        assertQuery(
                """
                        adjusted_rank
                        2
                        3
                        4
                        5
                        6
                        """,
                "SELECT row_number() OVER (ORDER BY ts) + 1 AS adjusted_rank FROM x",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts " +
                        "FROM long_sequence(5)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }
}
