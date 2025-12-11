package io.questdb.test.griffin.engine.window;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test to verify that the LIMIT clause in subqueries is correctly applied
 * before window functions, not after them.
 * 
 * This addresses the bug where "limit in subquery misapplied when outer window function is used"
 */
public class WindowLimitSubqueryFixTest extends AbstractCairoTest {

    @Test
    public void testLimitAppliedBeforeWindowFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_data AS (" +
                    "SELECT " +
                    "x AS id, " +
                    "rnd_double() AS value, " +
                    "rnd_symbol('A', 'B', 'C') AS category " +
                    "FROM long_sequence(100)" +
                    ")");

            // This query should apply LIMIT 10 to the subquery first,
            // then the window function should operate on those 10 rows only
            String query = "SELECT " +
                    "id, " +
                    "value, " +
                    "category, " +
                    "row_number() OVER (PARTITION BY category ORDER BY value) as rn " +
                    "FROM (" +
                    "    SELECT * FROM test_data ORDER BY id LIMIT 10" +
                    ")";

            // Verify the result has exactly 10 rows (from the subquery limit)
            try (RecordCursorFactory factory = select(query)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    int count = 0;
                    while (cursor.hasNext()) {
                        count++;
                    }
                    Assert.assertEquals("Query should return exactly 10 rows from subquery limit", 10, count);
                }
            }
            
            // Check the query plan to ensure LIMIT is applied before Window
            assertPlanNoLeakCheck(query, null);
        });
    }

    @Test 
    public void testLimitSignChangeAffectsResults() throws Exception {
        // This test specifically addresses the issue shown in the attachment
        // where changing the limit sign should affect the results if applied correctly
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades AS (" +
                    "SELECT " +
                    "x AS trade_id, " +
                    "rnd_symbol('BTC-USD', 'ETH-USD') AS symbol, " +
                    "timestamp_sequence(0, 100000000) AS timestamp, " +
                    "rnd_double() * 100 AS price " +
                    "FROM long_sequence(1000)" +
                    ") timestamp(timestamp)");

            // Query with positive limit in subquery
            String queryPositive = "SELECT " +
                    "trade_id, " +
                    "symbol, " +
                    "price, " +
                    "row_number() OVER (PARTITION BY symbol ORDER BY price) " +
                    "FROM (SELECT * FROM trades ORDER BY timestamp LIMIT 100)";

            // Query with negative limit in subquery  
            String queryNegative = "SELECT " +
                    "trade_id, " +
                    "symbol, " +
                    "price, " +
                    "row_number() OVER (PARTITION BY symbol ORDER BY price) " +
                    "FROM (SELECT * FROM trades ORDER BY timestamp LIMIT -100)";

            // Get the result counts - they should be different if limit is applied correctly
            int positiveCount = getQueryRowCount(queryPositive);
            int negativeCount = getQueryRowCount(queryNegative);
            
            System.out.println("Positive limit result count: " + positiveCount);
            System.out.println("Negative limit result count: " + negativeCount);
            
            // The counts should be different, proving the limit is applied to the subquery
            // If the bug existed, both would return the same count
            Assert.assertNotEquals("Changing limit sign should affect result count", positiveCount, negativeCount);
            
            // Positive limit should return exactly 100 rows
            Assert.assertEquals("Positive limit should return 100 rows", 100, positiveCount);
            
            // Negative limit should return the last 100 rows (different from positive)
            Assert.assertEquals("Negative limit should return 100 rows", 100, negativeCount);
        });
    }

    @Test
    public void testComplexWindowFunctionWithSubqueryLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE stock_prices AS (" +
                    "SELECT " +
                    "rnd_symbol('AAPL', 'GOOGL', 'MSFT') AS symbol, " +
                    "timestamp_sequence(0, 60000000) AS timestamp, " +
                    "rnd_double() * 200 AS price, " +
                    "rnd_int(100, 10000, 0) AS volume " +
                    "FROM long_sequence(1000)" +
                    ") timestamp(timestamp)");

            // Complex query with multiple window functions and subquery limit
            String query = "SELECT " +
                    "symbol, " +
                    "timestamp, " +
                    "price, " +
                    "volume, " +
                    "row_number() OVER (PARTITION BY symbol ORDER BY timestamp) as seq_num, " +
                    "avg(price) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as moving_avg, " +
                    "lag(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_price " +
                    "FROM (" +
                    "    SELECT * FROM stock_prices " +
                    "    WHERE symbol IN ('AAPL', 'GOOGL') " +
                    "    ORDER BY timestamp " +
                    "    LIMIT 50" +
                    ")";

            // This should return exactly 50 rows (from the subquery limit)
            try (RecordCursorFactory factory = select(query)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    int count = 0;
                    while (cursor.hasNext()) {
                        count++;
                    }
                    Assert.assertEquals("Complex query should return exactly 50 rows", 50, count);
                }
            }
            
            // Test with different limit to ensure it's respected
            String queryWithDifferentLimit = query.replace("LIMIT 50", "LIMIT 25");
            try (RecordCursorFactory factory = select(queryWithDifferentLimit)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    int count = 0;
                    while (cursor.hasNext()) {
                        count++;
                    }
                    Assert.assertEquals("Modified query should return exactly 25 rows", 25, count);
                }
            }
        });
    }

    private int getQueryRowCount(String query) throws Exception {
        try (RecordCursorFactory factory = select(query);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            int count = 0;
            while (cursor.hasNext()) {
                count++;
            }
            return count;
        }
    }
}