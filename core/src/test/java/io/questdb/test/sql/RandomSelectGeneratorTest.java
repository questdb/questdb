/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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

package io.questdb.test.sql;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for RandomSelectGenerator.
 */
public class RandomSelectGeneratorTest extends AbstractCairoTest {

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE empty_table (" +
                    "id INT, " +
                    "name STRING" +
                    ")");

            // No data inserted

            Rnd rnd = new Rnd(100, 200);
            RandomSelectGenerator generator = new RandomSelectGenerator(engine, rnd, "empty_table");

            // Should still generate valid queries even with no data
            for (int i = 0; i < 5; i++) {
                String query = generator.generate();
                LOG.info().$("Generated query on empty table: ").$(query).$();
                assertQueryNoLeakCheck(query);
            }
        });
    }

    @Test
    public void testGenerateMultipleQueriesDifferentResults() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE diverse (" +
                    "a INT, " +
                    "b STRING, " +
                    "c DOUBLE, " +
                    "d SYMBOL" +
                    ")");

            execute("INSERT INTO diverse SELECT " +
                    "x::int a, " +
                    "rnd_str('foo', 'bar', 'baz', 'qux') b, " +
                    "rnd_double() c, " +
                    "rnd_symbol('s1', 's2', 's3', 's4') d " +
                    "FROM long_sequence(20)");

            Rnd rnd = new Rnd(555, 555);
            RandomSelectGenerator generator = new RandomSelectGenerator(engine, rnd, "diverse");

            // Generate multiple queries and verify they're different
            String query1 = generator.generate();
            String query2 = generator.generate();
            String query3 = generator.generate();

            LOG.info().$("Query 1: ").$(query1).$();
            LOG.info().$("Query 2: ").$(query2).$();
            LOG.info().$("Query 3: ").$(query3).$();

            // Execute all to verify validity
            assertQueryNoLeakCheck(query1);
            assertQueryNoLeakCheck(query2);
            assertQueryNoLeakCheck(query3);

            // Verify they're not all identical (very unlikely with random generation)
            boolean allSame = query1.equals(query2) && query2.equals(query3);
            assertFalse("All generated queries are identical, expected variation", allSame);
        });
    }

    @Test
    public void testGenerateSimpleSelect() throws Exception {
        assertMemoryLeak(() -> {
            // Create a simple table
            execute("CREATE TABLE users (" +
                    "id INT, " +
                    "name STRING, " +
                    "age INT, " +
                    "active BOOLEAN, " +
                    "created TIMESTAMP" +
                    ") TIMESTAMP(created) PARTITION BY DAY");

            // Insert some test data
            execute("INSERT INTO users VALUES " +
                    "(1, 'Alice', 30, true, '2024-01-01T10:00:00.000000Z'), " +
                    "(2, 'Bob', 25, false, '2024-01-02T11:00:00.000000Z'), " +
                    "(3, 'Charlie', 35, true, '2024-01-03T12:00:00.000000Z')");

            // Create generator
            Rnd rnd = new Rnd(42, 42); // Fixed seed for reproducibility
            RandomSelectGenerator generator = new RandomSelectGenerator(engine, rnd, "users");

            // Generate and execute multiple random queries
            for (int i = 0; i < 10; i++) {
                String query = generator.generate();
                LOG.info().$("Generated query: ").$(query).$();

                // Verify the query is valid by executing it
                assertQueryNoLeakCheck(query);
            }
        });
    }

    @Test
    public void testGenerateWithAggregates() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with numeric columns
            execute("CREATE TABLE metrics (" +
                    "sensor_id INT, " +
                    "temperature DOUBLE, " +
                    "humidity INT, " +
                    "location SYMBOL, " +
                    "measured_at TIMESTAMP" +
                    ") TIMESTAMP(measured_at) PARTITION BY HOUR");

            // Insert test data
            execute("INSERT INTO metrics SELECT " +
                    "rnd_int(1, 5, 0) sensor_id, " +
                    "rnd_double() * 100 temperature, " +
                    "rnd_int(0, 100, 0) humidity, " +
                    "rnd_symbol('room1', 'room2', 'room3') location, " +
                    "timestamp_sequence('2024-01-01T00:00:00', 60000000L) measured_at " +
                    "FROM long_sequence(100)");

            // Create generator with high GROUP BY probability
            Rnd rnd = new Rnd(999, 888);
            RandomSelectGenerator generator = new RandomSelectGenerator(engine, rnd, "metrics");
            generator.setAggregationProbability(0.8); // High probability for aggregates

            // Generate and execute queries
            for (int i = 0; i < 10; i++) {
                String query = generator.generate();
                LOG.info().$("Generated aggregate query: ").$(query).$();

                // Execute to verify validity
                assertQueryNoLeakCheck(query);
            }
        });
    }

    @Test
    public void testGenerateWithCustomProbabilities() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE custom (" +
                    "id INT, " +
                    "value DOUBLE, " +
                    "tag SYMBOL" +
                    ")");

            execute("INSERT INTO custom SELECT " +
                    "x::int id, " +
                    "rnd_double() value, " +
                    "rnd_symbol('tag1', 'tag2', 'tag3') tag " +
                    "FROM long_sequence(30)");

            // Test with different probability configurations
            Rnd rnd = new Rnd(111, 222);

            // Configuration 1: Always include WHERE and ORDER BY
            RandomSelectGenerator gen1 = new RandomSelectGenerator(engine, rnd, "custom")
                    .setGenerateLiteralProbability(0.15)
                    .setWhereClauseProbability(1.0)
                    .setOrderByProbability(1.0)
                    .setLimitProbability(0.0)
                    .setAggregationProbability(0.0)
                    .setSampleByProbability(0.0);

            String query1 = gen1.generate();
            LOG.info().$("Query with WHERE+ORDER BY: ").$(query1).$();
            assertQueryNoLeakCheck(query1);

            // Verify it has WHERE and ORDER BY
            assertTrue(query1.contains("WHERE"));
            assertTrue(query1.contains("ORDER BY"));
            assertFalse(query1.contains("GROUP BY"));

            // Configuration 2: Always include LIMIT
            rnd = new Rnd(333, 444);
            RandomSelectGenerator gen2 = new RandomSelectGenerator(engine, rnd, "custom")
                    .setWhereClauseProbability(0.0)
                    .setOrderByProbability(1.0)
                    .setLimitProbability(1.0)
                    .setAggregationProbability(0.0)
                    .setSampleByProbability(0.0);

            String query2 = gen2.generate();
            LOG.info().$("Query with LIMIT: ").$(query2).$();
            assertQueryNoLeakCheck(query2);

            // Verify it has LIMIT
            assertTrue(query2.contains("LIMIT"));
        });
    }

    @Test
    public void testGenerateWithMultipleTables() throws Exception {
        assertMemoryLeak(() -> {
            // Create multiple tables
            execute("CREATE TABLE products (" +
                    "product_id INT, " +
                    "product_name STRING, " +
                    "price DOUBLE, " +
                    "category SYMBOL" +
                    ")");

            execute("CREATE TABLE orders (" +
                    "order_id INT, " +
                    "product_id INT, " +
                    "quantity INT, " +
                    "order_date TIMESTAMP" +
                    ") TIMESTAMP(order_date) PARTITION BY MONTH");

            // Insert test data
            execute("INSERT INTO products VALUES " +
                    "(1, 'Widget', 9.99, 'gadgets'), " +
                    "(2, 'Gizmo', 19.99, 'gadgets'), " +
                    "(3, 'Doohickey', 29.99, 'tools')");

            execute("INSERT INTO orders VALUES " +
                    "(101, 1, 5, '2024-01-15T10:00:00.000000Z'), " +
                    "(102, 2, 3, '2024-01-16T11:00:00.000000Z'), " +
                    "(103, 1, 2, '2024-01-17T12:00:00.000000Z')");

            // Create generator with multiple tables
            Rnd rnd = new Rnd(123, 456);
            RandomSelectGenerator generator = new RandomSelectGenerator(engine, rnd, "products", "orders")
                    .setJoinProbability(0.5); // Increase JOIN probability

            // Generate and execute queries
            for (int i = 0; i < 15; i++) {
                String query = generator.generate();
                LOG.info().$("Generated multi-table query: ").$(query).$();

                // Execute to verify validity
                assertQueryNoLeakCheck(query);
            }
        });
    }

    @Test
    public void testGenerateWithVariousColumnTypes() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with various column types
            execute("CREATE TABLE mixed_types (" +
                    "bool_col BOOLEAN, " +
                    "byte_col BYTE, " +
                    "short_col SHORT, " +
                    "int_col INT, " +
                    "long_col LONG, " +
                    "float_col FLOAT, " +
                    "double_col DOUBLE, " +
                    "string_col STRING, " +
                    "symbol_col SYMBOL, " +
                    "timestamp_col TIMESTAMP, " +
                    "char_col CHAR" +
                    ") TIMESTAMP(timestamp_col)");

            // Insert test data using random functions
            execute("INSERT INTO mixed_types SELECT " +
                    "rnd_boolean() bool_col, " +
                    "rnd_byte() byte_col, " +
                    "rnd_short() short_col, " +
                    "rnd_int() int_col, " +
                    "rnd_long() long_col, " +
                    "rnd_float() float_col, " +
                    "rnd_double() double_col, " +
                    "rnd_str('foo', 'bar', 'baz') string_col, " +
                    "rnd_symbol('A', 'B', 'C') symbol_col, " +
                    "timestamp_sequence('2024-01-01', 1000000L) timestamp_col, " +
                    "rnd_char() char_col " +
                    "FROM long_sequence(50)");

            // Create generator
            Rnd rnd = new Rnd(777, 666);
            RandomSelectGenerator generator = new RandomSelectGenerator(engine, rnd, "mixed_types")
                    .setWhereClauseProbability(0.7)
                    .setOrderByProbability(0.5)
                    .setLimitProbability(0.4);

            // Generate and execute queries
            for (int i = 0; i < 20; i++) {
                String query = generator.generate();
                LOG.info().$("Generated query with mixed types: ").$(query).$();

                // Execute to verify validity
                assertQueryNoLeakCheck(query);
            }
        });
    }

    @Test
    public void testGroupByBooleanAvoidsLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE bool_only (" +
                    "flag BOOLEAN" +
                    ")");

            execute("INSERT INTO bool_only VALUES " +
                    "(true), (false), (true), (false)");

            Rnd rnd = TestUtils.generateRandom(LOG);
            RandomSelectGenerator generator = new RandomSelectGenerator(engine, rnd, "bool_only")
                    .setAggregationProbability(1.0) // always aggregate
                    .setSampleByProbability(0.0)
                    .setOrderByProbability(1.0) // always attempt ORDER BY / LIMIT
                    .setLimitProbability(1.0)
                    .setWhereClauseProbability(0.0)
                    .setJoinProbability(0.0);

            String query = generator.generate();
            LOG.info().$("Query with boolean GROUP BY key (no LIMIT expected): ").$(query).$();

            assertTrue(query.contains("GROUP BY"));
            assertFalse("LIMIT should not be generated when grouping on boolean key", query.contains("LIMIT"));
            assertQueryNoLeakCheck(query);
        });
    }

    @Test
    public void testGroupByOrderableKeyAllowsLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ints_only (" +
                    "v INT" +
                    ")");

            execute("INSERT INTO ints_only VALUES (1), (2), (3), (4)");

            Rnd rnd = TestUtils.generateRandom(LOG);
            RandomSelectGenerator generator = new RandomSelectGenerator(engine, rnd, "ints_only")
                    .setAggregationProbability(1.0)
                    .setSampleByProbability(0.0)
                    .setOrderByProbability(1.0)
                    .setLimitProbability(1.0)
                    .setWhereClauseProbability(0.0)
                    .setJoinProbability(0.0);

            String query = generator.generate();
            LOG.info().$("Query with orderable GROUP BY key (LIMIT expected): ").$(query).$();

            assertTrue(query.contains("GROUP BY"));
            assertTrue("LIMIT should be generated when grouping on orderable key", query.contains("LIMIT"));
            assertTrue("ORDER BY should be present when LIMIT is generated", query.contains("ORDER BY"));
            assertQueryNoLeakCheck(query);
        });
    }

    @Test
    public void testSampleByOrderEnforcesAscWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sample_table (" +
                    "ts TIMESTAMP, " +
                    "v INT" +
                    ") TIMESTAMP(ts)");

            execute("INSERT INTO sample_table SELECT " +
                    "timestamp_sequence('2024-01-01', 1000000L) ts, " +
                    "x::int v " +
                    "FROM long_sequence(10)");

            Rnd rnd = TestUtils.generateRandom(LOG);
            RandomSelectGenerator generator = new RandomSelectGenerator(engine, rnd, "sample_table")
                    .setAggregationProbability(1.0)
                    .setSampleByProbability(1.0) // always SAMPLE BY
                    .setOrderByProbability(1.0)
                    .setLimitProbability(1.0)
                    .setWhereClauseProbability(0.0)
                    .setJoinProbability(0.0);

            String query = generator.generate();
            LOG.info().$("SAMPLE BY query with LIMIT: ").$(query).$();

            assertTrue(query.contains("SAMPLE BY"));
            assertTrue(query.contains("ORDER BY 1"));
            assertFalse("DESC should not be used with SAMPLE BY ordering", query.contains("DESC"));
            assertTrue(query.contains("LIMIT"));
            assertQueryNoLeakCheck(query);
        });
    }

    // Helper method to execute query without checking specific results
    private void assertQueryNoLeakCheck(String query) throws Exception {
        try (RecordCursorFactory factory = select(query)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                // Fetch the first record to verify query is valid
                cursor.hasNext();
            }
        }
    }
}
