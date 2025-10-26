package io.questdb.test.griffin.engine.window;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class WindowLimitSubqueryBugTest extends AbstractCairoTest {

    @Test
    public void testLimitSubqueryMisapplicationFromAttachment() throws Exception {
        // This test reproduces the exact issue shown in the attachment
        // where changing the sign of the limit doesn't change the result
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades AS (" +
                    "SELECT " +
                    "x," +
                    "rnd_symbol('BTC-USD', 'ETH-USD', 'LTC-USD') AS symbol, " +
                    "timestamp_sequence('2022-01-01T00:00:00.000000Z', 1000000L) AS timestamp, " +
                    "rnd_double() * 100 AS price, " +
                    "rnd_int(1, 1000, 0) AS amount " +
                    "FROM long_sequence(1000000)" +
                    ") timestamp(timestamp)");

            // Query similar to the one in the attachment with LIMIT in subquery + window function
            String queryWithPositiveLimit = "SELECT *, " +
                    "row_number() OVER (PARTITION BY timestamp_floor('h', timestamp), symbol " +
                    "ORDER BY timestamp) " +
                    "FROM (SELECT * FROM trades LIMIT 1000000) " +
                    "WHERE x = 1";

            // Query with LIMIT -1000000 in subquery + window function  
            String queryWithNegativeLimit = "SELECT *, " +
                    "row_number() OVER (PARTITION BY timestamp_floor('h', timestamp), symbol " +
                    "ORDER BY timestamp) " +
                    "FROM (SELECT * FROM trades LIMIT -1000000) " +
                    "WHERE x = 1";

            // Check the query plans to see where the limit is applied
            assertPlanNoLeakCheck(queryWithPositiveLimit, null);
            assertPlanNoLeakCheck(queryWithNegativeLimit, null);
            
            // The limit should appear in the subquery part, not in the outer window part
            // If the bug is fixed, we should see "Limit" before "Window" in the plan
        });
    }

    @Test
    public void testSimpleLimitSubqueryWithWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table AS (" +
                    "SELECT " +
                    "x AS id, " +
                    "rnd_double() AS value " +
                    "FROM long_sequence(1000)" +
                    ")");

            // Simple case: subquery with LIMIT should only return 5 rows
            // Window function should operate on those 5 rows only
            String query = "SELECT " +
                    "*, " +
                    "row_number() OVER (ORDER BY value) " +
                    "FROM (" +
                    "SELECT * FROM test_table ORDER BY id LIMIT 5" +
                    ")";

            // Verify that the result has exactly 5 rows
            try (RecordCursorFactory factory = select(query)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    int count = 0;
                    while (cursor.hasNext()) {
                        count++;
                    }
                    Assert.assertEquals(5, count);
                }
            }
            
            // Check the query plan to see where the limit is applied
            assertPlanNoLeakCheck(query, null);
        });
    }

    @Test
    public void testComplexWindowFunctionWithSubqueryLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades AS (" +
                    "SELECT " +
                    "rnd_symbol('BTC-USD', 'ETH-USD') AS symbol, " +
                    "timestamp_sequence(0, 100000000) AS timestamp, " +
                    "rnd_double() * 100 AS price " +
                    "FROM long_sequence(100)" +
                    ") timestamp(timestamp)");

            // Query that should apply LIMIT to subquery first, then window function
            String query = "SELECT " +
                    "symbol, " +
                    "timestamp, " +
                    "price, " +
                    "row_number() OVER (PARTITION BY symbol ORDER BY price) as rn " +
                    "FROM (" +
                    "SELECT * FROM trades ORDER BY timestamp LIMIT 10" +
                    ")";

            // This should return exactly 10 rows (the limit from the subquery)
            try (RecordCursorFactory factory = select(query)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    int count = 0;
                    while (cursor.hasNext()) {
                        count++;
                    }
                    Assert.assertEquals(10, count);
                }
            }
            
            // Now test with different limit values to ensure they're respected
            String queryLimit5 = query.replace("LIMIT 10", "LIMIT 5");
            try (RecordCursorFactory factory = select(queryLimit5)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    int count = 0;
                    while (cursor.hasNext()) {
                        count++;
                    }
                    Assert.assertEquals(5, count);
                }
            }
            
            String queryLimit15 = query.replace("LIMIT 10", "LIMIT 15");
            try (RecordCursorFactory factory = select(queryLimit15)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    int count = 0;
                    while (cursor.hasNext()) {
                        count++;
                    }
                    Assert.assertEquals(15, count);
                }
            }
        });
    }
}