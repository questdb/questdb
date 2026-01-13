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
        // Test window function result used inside CASE expression via subquery
        assertQuery(
                """
                        category
                        first
                        middle
                        last
                        """,
                "SELECT CASE " +
                        "  WHEN rn = 1 THEN 'first' " +
                        "  WHEN rn = 3 THEN 'last' " +
                        "  ELSE 'middle' " +
                        "END AS category " +
                        "FROM (SELECT row_number() OVER (ORDER BY ts) AS rn FROM x)",
                "CREATE TABLE x AS (" +
                        "SELECT timestamp_sequence('2024-01-01', 1000000) AS ts FROM long_sequence(3)" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                null,
                false,
                true
        );
    }
}
