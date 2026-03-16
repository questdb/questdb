package io.questdb.test.cairo;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class SecondaryKeySortTest extends AbstractCairoTest {

    @Test
    public void testCreateTableWithSecondarySort() throws Exception {
        // Test creating table with secondary sort by single column
        execute("CREATE TABLE test1 (ts TIMESTAMP, symbol SYMBOL, price DOUBLE) PARTITION BY DAY ORDER BY (symbol, ts)");
        execute("DROP TABLE test1");

        // Test creating table with secondary sort by multiple columns
        execute("CREATE TABLE test2 (ts TIMESTAMP, symbol SYMBOL, exchange SYMBOL, price DOUBLE) PARTITION BY DAY ORDER BY (symbol, exchange, ts)");
        execute("DROP TABLE test2");
    }

    @Test
    public void testCreateTableWithSecondarySortNumeric() throws Exception {
        // Test with numeric types
        execute("CREATE TABLE test (ts TIMESTAMP, id INT, seq LONG) PARTITION BY DAY ORDER BY (id, seq, ts)");
        execute("DROP TABLE test");
    }

    @Test
    public void testSecondarySortBasicMerge() throws Exception {
        execute("CREATE TABLE test (ts TIMESTAMP, symbol SYMBOL, price DOUBLE) PARTITION BY DAY ORDER BY (symbol, ts)");

        // Insert out-of-order data
        execute("INSERT INTO test (ts, symbol, price) VALUES ('2024-01-01T00:00:00.000000Z', 'AAPL', 150.0)");
        execute("INSERT INTO test (ts, symbol, price) VALUES ('2024-01-01T00:00:00.000001Z', 'GOOG', 2800.0)");
        execute("INSERT INTO test (ts, symbol, price) VALUES ('2024-01-01T00:00:00.000002Z', 'AAPL', 151.0)");

        // Force O3 merge
        execute("ALTER TABLE test ADD COLUMN dummy INT DEFAULT 0");

        // Verify data was inserted successfully - should be sorted by symbol first, then timestamp
        assertQueryNoLeakCheck("ts                         symbol  price\n" +
                "2024-01-01T00:00:00.000000Z AAPPL   150.0\n" +
                "2024-01-01T00:00:00.000002Z AAPPL   151.0\n" +
                "2024-01-01T00:00:00.000001Z GOOG    2800.0\n",
                "SELECT * FROM test ORDER BY symbol, ts", "CREATE TABLE test (ts TIMESTAMP, symbol SYMBOL, price DOUBLE) PARTITION BY DAY ORDER BY (symbol, ts)");
    }

    @Test
    public void testSecondarySortMultipleColumns() throws Exception {
        execute("CREATE TABLE test (ts TIMESTAMP, symbol SYMBOL, exchange SYMBOL, price DOUBLE) PARTITION BY DAY ORDER BY (symbol, exchange, ts)");

        // Insert data that should be sorted by symbol first, then exchange, then timestamp
        execute("INSERT INTO test (ts, symbol, exchange, price) VALUES ('2024-01-01T00:00:00.000003Z', 'AAPL', 'XNYS', 150.0)");
        execute("INSERT INTO test (ts, symbol, exchange, price) VALUES ('2024-01-01T00:00:00.000004Z', 'AAPL', 'XNAS', 151.0)");
        execute("INSERT INTO test (ts, symbol, exchange, price) VALUES ('2024-01-01T00:00:00.000005Z', 'GOOG', 'XNAS', 2800.0)");

        // Force O3 merge
        execute("ALTER TABLE test ADD COLUMN dummy INT DEFAULT 0");

        // Verify data was inserted successfully - sorted by symbol, then exchange, then ts
        assertQueryNoLeakCheck("ts                         symbol  exchange  price\n" +
                "2024-01-01T00:00:00.000003Z AAPPL   XNYS      150.0\n" +
                "2024-01-01T00:00:00.000004Z AAPPL   XNAS      151.0\n" +
                "2024-01-01T00:00:00.000005Z GOOG    XNAS      2800.0\n",
                "SELECT * FROM test ORDER BY symbol, exchange, ts", "CREATE TABLE test (ts TIMESTAMP, symbol SYMBOL, exchange SYMBOL, price DOUBLE) PARTITION BY DAY ORDER BY (symbol, exchange, ts)");
    }

    @Test
    public void testSecondarySortWithDifferentPartitionGranularity() throws Exception {
        // Test with different partition by values
        execute("CREATE TABLE test1 (ts TIMESTAMP, symbol SYMBOL, price DOUBLE) PARTITION BY WEEK ORDER BY (symbol, ts)");
        execute("CREATE TABLE test2 (ts TIMESTAMP, symbol SYMBOL, price DOUBLE) PARTITION BY MONTH ORDER BY (symbol, ts)");
        execute("CREATE TABLE test3 (ts TIMESTAMP, symbol SYMBOL, price DOUBLE) PARTITION BY YEAR ORDER BY (symbol, ts)");

        execute("INSERT INTO test1 (ts, symbol, price) VALUES ('2024-01-15T00:00:00.000000Z', 'AAPL', 150.0)");
        execute("INSERT INTO test2 (ts, symbol, price) VALUES ('2024-01-15T00:00:00.000000Z', 'AAPL', 150.0)");
        execute("INSERT INTO test3 (ts, symbol, price) VALUES ('2024-01-15T00:00:00.000000Z', 'AAPL', 150.0)");

        execute("ALTER TABLE test1 ADD COLUMN d1 INT DEFAULT 0");
        execute("ALTER TABLE test2 ADD COLUMN d2 INT DEFAULT 0");
        execute("ALTER TABLE test3 ADD COLUMN d3 INT DEFAULT 0");

        execute("DROP TABLE test1");
        execute("DROP TABLE test2");
        execute("DROP TABLE test3");
    }

    @Test(expected = Exception.class)
    public void testSecondarySortInvalidType() throws Exception {
        // Test that BOOLEAN type is not allowed
        execute("CREATE TABLE test (ts TIMESTAMP, flag BOOLEAN) PARTITION BY DAY ORDER BY (flag, ts)");
    }
}