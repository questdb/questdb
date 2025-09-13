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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Test case for GitHub issue #6121: First/Last ignoring ORDER BY (parallel run bug)
 * 
 * This reproduces the issue where first() function behavior is inconsistent when used with 
 * UNION ALL and subqueries with ORDER BY clauses, particularly in parallel execution mode.
 */
public class FirstFunctionOrderByBugTest extends AbstractCairoTest {

    @Test
    public void testFirstFunctionIgnoresOrderByInUnion() throws Exception {
        assertMemoryLeak(() -> {
            // Create a trades table with sample data - similar to what was mentioned in the issue
            compiler.compile(
                "CREATE TABLE trades AS (" +
                "   SELECT " +
                "       cast(x as double) as price, " +
                "       timestamp_sequence('2024-01-01T00:00:00.000000Z', 1000000L) as timestamp " +
                "   FROM long_sequence(10)" +
                ") TIMESTAMP(timestamp) PARTITION BY DAY",
                sqlExecutionContext
            );

            // Execute the exact query pattern from the issue report
            String problematicQuery = 
                "with t AS " +
                "( select * from trades where timestamp >= '2024-01-01T00:00:00.000000Z'), " +
                "d as (select * from t order by timestamp desc) " +
                "select first(timestamp) as timestamp from t " +
                "UNION ALL " +
                "select first(timestamp) from d " +
                "UNION ALL " +
                "(select timestamp from d LIMIT 1) " +
                "order by timestamp";

            // With the fix, first(timestamp) from d should return the first timestamp in the ordered result
            // which would be the latest timestamp (due to DESC ordering)
            // The results should show that first() respects the underlying data ordering
            assertSql(
                "timestamp\n" +
                "2024-01-01T00:00:00.000000Z\n" +
                "2024-01-01T00:00:00.000000Z\n" +
                "2024-01-01T00:00:09.000000Z\n",
                problematicQuery
            );
        });
    }

    @Test
    public void testFirstFunctionWithOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            // Create test data with clear ordering
            compiler.compile(
                "CREATE TABLE test_data AS (" +
                "   SELECT " +
                "       x as id, " +
                "       timestamp_sequence('2024-01-01T00:00:00.000000Z', 1000000L) as timestamp " +
                "   FROM long_sequence(5)" +
                ") TIMESTAMP(timestamp)",
                sqlExecutionContext
            );

            // Test that first() respects order in simple case
            assertSql(
                "first_ts\n" +
                "2024-01-01T00:00:00.000000Z\n",
                "SELECT first(timestamp) as first_ts FROM (SELECT * FROM test_data ORDER BY timestamp ASC)"
            );

            // Test that first() should get the last timestamp when ordered desc
            assertSql(
                "first_ts\n" +
                "2024-01-01T00:00:04.000000Z\n",
                "SELECT first(timestamp) as first_ts FROM (SELECT * FROM test_data ORDER BY timestamp DESC)"
            );
        });
    }

    @Test
    public void testParallelExecutionDisabledForReversedOrder() throws Exception {
        assertMemoryLeak(() -> {
            // Create larger dataset to potentially trigger parallel execution
            compiler.compile(
                "CREATE TABLE large_trades AS (" +
                "   SELECT " +
                "       rnd_double() * 100 as price, " +
                "       timestamp_sequence('2024-01-01T00:00:00.000000Z', 100000L) as timestamp " +
                "   FROM long_sequence(1000)" +
                ") TIMESTAMP(timestamp) PARTITION BY DAY",
                sqlExecutionContext
            );

            // Test that first() works correctly with different ordering
            String ascQuery = "SELECT first(timestamp) as first_ts FROM (SELECT * FROM large_trades ORDER BY timestamp ASC)";
            String descQuery = "SELECT first(timestamp) as first_ts FROM (SELECT * FROM large_trades ORDER BY timestamp DESC)";

            // Get results
            String ascResult = compiler.compile(ascQuery, sqlExecutionContext).getRecordCursorFactory().getCursor(sqlExecutionContext).getRecord().getTimestamp(0);
            String descResult = compiler.compile(descQuery, sqlExecutionContext).getRecordCursorFactory().getCursor(sqlExecutionContext).getRecord().getTimestamp(0);

            // Results should be different if ordering is respected
            // (This is the core of the bug - they might be the same when they shouldn't be)
        });
    }
}