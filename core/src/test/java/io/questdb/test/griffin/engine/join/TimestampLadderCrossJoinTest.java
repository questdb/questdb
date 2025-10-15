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

package io.questdb.test.griffin.engine.join;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Tests for the Timestamp Ladder Join optimization.
 * This optimization applies to queries that:
 * 1. Cross-join a table with an arithmetic sequence
 * 2. Add the sequence offset to a timestamp column
 * 3. Order by the resulting timestamp
 * <p>
 * The optimizer recognizes this pattern and emits rows directly in sorted order
 * without materializing the full cross-join result.
 */
public class TimestampLadderCrossJoinTest extends AbstractCairoTest {

    @Test
    public void testDuplicateTimestampsInMaster() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, '1970-01-01T00:00:00.000000Z')");
            execute("INSERT INTO orders VALUES (2, '1970-01-01T00:00:00.000000Z')");
            execute("INSERT INTO orders VALUES (3, '1970-01-01T00:00:00.000000Z')");

            assertQueryNoLeakCheck(
                    """
                            id\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:00:00.000000Z
                            3\t1970-01-01T00:00:00.000000Z
                            1\t1970-01-01T00:00:01.000000Z
                            2\t1970-01-01T00:00:01.000000Z
                            3\t1970-01-01T00:00:01.000000Z
                            """,
                    """
                            WITH offsets AS (
                                SELECT x-1 AS sec_offs, 1_000_000 * (x-1) AS usec_offs
                                FROM long_sequence(2)
                            )
                            SELECT id, order_ts + usec_offs AS ts
                            FROM orders CROSS JOIN offsets
                            ORDER BY order_ts + usec_offs""",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmptyMasterCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            // No data inserted

            assertQueryNoLeakCheck(
                    "id\tts\n",
                    """
                            WITH offsets AS (
                                SELECT x-1 AS sec_offs, 1_000_000 * (x-1) AS usec_offs FROM long_sequence(3)
                            )
                            SELECT id, order_ts + usec_offs AS ts
                            FROM orders CROSS JOIN offsets
                            ORDER BY order_ts + usec_offs""",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmptySlaveCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, '1970-01-01T00:00:00.000000Z')");

            assertQueryNoLeakCheck(
                    "id\tts\n",
                    """
                            WITH offsets AS (
                                SELECT x-1 AS sec_offs, 1_000_000 * (x-1) AS usec_offs
                                FROM long_sequence(0)
                            )
                            SELECT id, order_ts + usec_offs AS ts
                            FROM orders CROSS JOIN offsets
                            ORDER BY order_ts + usec_offs""",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testLargeCrossProduct() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            // Insert 10 master rows with 1-second spacing
            for (int i = 1; i <= 10; i++) {
                execute("INSERT INTO orders VALUES (" + i + ", " + (i * 1_000_000_000L) + ")");
            }

            // 100-row sequence creates 1000 total rows
            String sql = "WITH offsets AS (\n" +
                    "    SELECT x-1 AS sec_offs, 1_000_000 * (x-1) usec_offs\n" +
                    "    FROM long_sequence(100)\n" +
                    ")\n" +
                    "SELECT id, order_ts + usec_offs AS ts\n" +
                    "FROM orders CROSS JOIN offsets\n" +
                    "ORDER BY order_ts + usec_offs";

            assertQueryNoLeakCheck(
                    "count\n" +
                            "1000\n",
                    "SELECT count(*) FROM (" + sql + ")",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testManyMasterRowsSmallSequence() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            // Insert 100 master rows
            for (int i = 1; i <= 100; i++) {
                execute("INSERT INTO orders VALUES (" + i + ", " + (i * 1_000_000L) + ")");
            }

            String sql = "WITH offsets AS (\n" +
                    "    SELECT x-1 AS sec_offs, 1_000_000 * (x-1) usec_offs\n" +
                    "    FROM long_sequence(3)\n" +
                    ")\n" +
                    "SELECT id, order_ts + usec_offs AS ts\n" +
                    "FROM orders CROSS JOIN offsets\n" +
                    "ORDER BY order_ts + usec_offs";

            // Verify count
            assertQueryNoLeakCheck(
                    "count\n" +
                            "300\n",
                    "SELECT count(*) FROM (" + sql + ")",
                    null,
                    false,
                    true
            );

            // Verify first and last rows
            assertQueryNoLeakCheck(
                    "id\tts\n" +
                            "1\t1970-01-01T00:00:00.001000Z\n",
                    "SELECT id, ts FROM (" + sql + ") LIMIT 1",
                    null,
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    "id\tts\n" +
                            "100\t1970-01-01T00:00:00.102000Z\n",
                    "SELECT id, ts FROM (" + sql + ") LIMIT -1",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMasterWithFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, '1970-01-01T00:00:00.000000Z')");
            execute("INSERT INTO orders VALUES (2, '1970-01-01T00:00:05.000000Z')");
            execute("INSERT INTO orders VALUES (3, '1970-01-01T00:00:10.000000Z')");

            assertQueryNoLeakCheck(
                    "id\tts\n" +
                            "1\t1970-01-01T00:00:00.000000Z\n" +
                            "1\t1970-01-01T00:00:01.000000Z\n" +
                            "3\t1970-01-01T00:00:10.000000Z\n" +
                            "3\t1970-01-01T00:00:11.000000Z\n",
                    "WITH offsets AS (\n" +
                            "    SELECT x-1 AS sec_offs, 1_000_000 * (x-1) usec_offs\n" +
                            "    FROM long_sequence(2)\n" +
                            ")\n" +
                            "SELECT id, order_ts + usec_offs AS ts\n" +
                            "FROM orders CROSS JOIN offsets\n" +
                            "WHERE id != 2\n" +
                            "ORDER BY order_ts + usec_offs",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMultipleMasterRowsHighlyInterleaved() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            // Master rows close together (100ms apart)
            execute("INSERT INTO orders VALUES (1, '1970-01-01T00:00:00.000000Z')");
            execute("INSERT INTO orders VALUES (2, '1970-01-01T00:00:00.100000Z')");
            execute("INSERT INTO orders VALUES (3, '1970-01-01T00:00:00.200000Z')");

            // Large sequence (1-second offsets) creates heavy interleaving
            assertQueryNoLeakCheck(
                    "id\tts\n" +
                            "1\t1970-01-01T00:00:00.000000Z\n" +
                            "2\t1970-01-01T00:00:00.100000Z\n" +
                            "3\t1970-01-01T00:00:00.200000Z\n" +
                            "1\t1970-01-01T00:00:01.000000Z\n" +
                            "2\t1970-01-01T00:00:01.100000Z\n" +
                            "3\t1970-01-01T00:00:01.200000Z\n" +
                            "1\t1970-01-01T00:00:02.000000Z\n" +
                            "2\t1970-01-01T00:00:02.100000Z\n" +
                            "3\t1970-01-01T00:00:02.200000Z\n",
                    "WITH offsets AS (\n" +
                            "    SELECT x-1 AS sec_offs, 1_000_000 * (x-1) usec_offs\n" +
                            "    FROM long_sequence(3)\n" +
                            ")\n" +
                            "SELECT id, order_ts + usec_offs AS ts\n" +
                            "FROM orders CROSS JOIN offsets\n" +
                            "ORDER BY order_ts + usec_offs",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMultipleMasterRowsInterleaved() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, '1970-01-01T00:00:00.000000Z')");
            execute("INSERT INTO orders VALUES (2, '1970-01-01T00:00:01.000000Z')");
            execute("INSERT INTO orders VALUES (3, '1970-01-01T00:00:02.000000Z')");

            // With 5-second offsets (0, 1, 2, 3, 4), the sequences interleave
            assertQueryNoLeakCheck(
                    "id\tts\n" +
                            "1\t1970-01-01T00:00:00.000000Z\n" +
                            "1\t1970-01-01T00:00:01.000000Z\n" +
                            "2\t1970-01-01T00:00:01.000000Z\n" +
                            "1\t1970-01-01T00:00:02.000000Z\n" +
                            "2\t1970-01-01T00:00:02.000000Z\n" +
                            "3\t1970-01-01T00:00:02.000000Z\n" +
                            "1\t1970-01-01T00:00:03.000000Z\n" +
                            "2\t1970-01-01T00:00:03.000000Z\n" +
                            "3\t1970-01-01T00:00:03.000000Z\n" +
                            "1\t1970-01-01T00:00:04.000000Z\n" +
                            "2\t1970-01-01T00:00:04.000000Z\n" +
                            "3\t1970-01-01T00:00:04.000000Z\n" +
                            "2\t1970-01-01T00:00:05.000000Z\n" +
                            "3\t1970-01-01T00:00:05.000000Z\n" +
                            "3\t1970-01-01T00:00:06.000000Z\n",
                    "WITH offsets AS (\n" +
                            "    SELECT x-1 AS sec_offs, 1_000_000 * (x-1) usec_offs\n" +
                            "    FROM long_sequence(5)\n" +
                            ")\n" +
                            "SELECT id, order_ts + usec_offs AS ts\n" +
                            "FROM orders CROSS JOIN offsets\n" +
                            "ORDER BY order_ts + usec_offs",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMultipleMasterRowsNonOverlapping() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, '1970-01-01T00:00:00.000000Z')");
            execute("INSERT INTO orders VALUES (2, '1970-01-01T00:01:00.000000Z')");
            execute("INSERT INTO orders VALUES (3, '1970-01-01T00:02:00.000000Z')");

            // With 3-second offsets (0, 1, 2), the sequences don't overlap
            assertQueryNoLeakCheck(
                    "id\tts\n" +
                            "1\t1970-01-01T00:00:00.000000Z\n" +
                            "1\t1970-01-01T00:00:01.000000Z\n" +
                            "1\t1970-01-01T00:00:02.000000Z\n" +
                            "2\t1970-01-01T00:01:00.000000Z\n" +
                            "2\t1970-01-01T00:01:01.000000Z\n" +
                            "2\t1970-01-01T00:01:02.000000Z\n" +
                            "3\t1970-01-01T00:02:00.000000Z\n" +
                            "3\t1970-01-01T00:02:01.000000Z\n" +
                            "3\t1970-01-01T00:02:02.000000Z\n",
                    "WITH offsets AS (\n" +
                            "    SELECT x-1 AS sec_offs, 1_000_000 * (x-1) usec_offs\n" +
                            "    FROM long_sequence(3)\n" +
                            ")\n" +
                            "SELECT id, order_ts + usec_offs AS ts\n" +
                            "FROM orders CROSS JOIN offsets\n" +
                            "ORDER BY order_ts + usec_offs",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testNegativeOffsets() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, '1970-01-01T00:00:05.000000Z')");

            assertQueryNoLeakCheck(
                    "id\tts\n" +
                            "1\t1970-01-01T00:00:03.000000Z\n" +
                            "1\t1970-01-01T00:00:04.000000Z\n" +
                            "1\t1970-01-01T00:00:05.000000Z\n" +
                            "1\t1970-01-01T00:00:06.000000Z\n" +
                            "1\t1970-01-01T00:00:07.000000Z\n",
                    "WITH offsets AS (\n" +
                            "    SELECT x-3 AS sec_offs, 1_000_000 * (x-3) usec_offs\n" +
                            "    FROM long_sequence(5)\n" +
                            ")\n" +
                            "SELECT id, order_ts + usec_offs AS ts\n" +
                            "FROM orders CROSS JOIN offsets\n" +
                            "ORDER BY order_ts + usec_offs",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testOptimizationApplied() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, '1970-01-01T00:00:00.000000Z')");

            assertPlanNoLeakCheck(
                    "WITH offsets AS (\n" +
                            "    SELECT x-1 AS sec_offs, 1_000_000 * (x-1) usec_offs\n" +
                            "    FROM long_sequence(3)\n" +
                            ")\n" +
                            "SELECT id, order_ts + usec_offs AS ts\n" +
                            "FROM orders CROSS JOIN offsets\n" +
                            "ORDER BY order_ts + usec_offs",
                    "VirtualRecord\n" +
                            "  functions: [id,order_ts+usec_offs]\n" +
                            "    Timestamp Ladder Join\n" +
                            "      timestampColumn: 1\n" +
                            "      slaveColumn: 1\n"
            );
        });
    }

    @Test
    public void testOptimizationNotAppliedWhenNoOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, '1970-01-01T00:00:00.000000Z')");

            // Without ORDER BY, optimization should not be applied
            assertPlanNoLeakCheck(
                    "WITH offsets AS (\n" +
                            "    SELECT x-1 AS sec_offs, 1_000_000 * (x-1) usec_offs\n" +
                            "    FROM long_sequence(3)\n" +
                            ")\n" +
                            "SELECT id, order_ts + usec_offs AS ts\n" +
                            "FROM orders CROSS JOIN offsets",
                    "VirtualRecord\n" +
                            "  functions: [id,order_ts+usec_offs]\n" +
                            "    Cross Join\n"
            );
        });
    }

    @Test
    public void testRealWorldScenario() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sensor_readings (\n" +
                    "    sensor_id INT,\n" +
                    "    temperature DOUBLE,\n" +
                    "    reading_ts TIMESTAMP\n" +
                    ") TIMESTAMP(reading_ts)");

            execute("INSERT INTO sensor_readings VALUES (101, 20.5, '2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO sensor_readings VALUES (102, 21.3, '2024-01-01T00:00:30.000000Z')");
            execute("INSERT INTO sensor_readings VALUES (103, 19.8, '2024-01-01T00:01:00.000000Z')");

            // Simulate generating interpolated timestamps every 10 seconds for each reading
            assertQueryNoLeakCheck(
                    "sensor_id\ttemperature\tts\n" +
                            "101\t20.5\t2024-01-01T00:00:00.000000Z\n" +
                            "101\t20.5\t2024-01-01T00:00:10.000000Z\n" +
                            "101\t20.5\t2024-01-01T00:00:20.000000Z\n" +
                            "102\t21.3\t2024-01-01T00:00:30.000000Z\n" +
                            "101\t20.5\t2024-01-01T00:00:30.000000Z\n" +
                            "102\t21.3\t2024-01-01T00:00:40.000000Z\n" +
                            "101\t20.5\t2024-01-01T00:00:40.000000Z\n" +
                            "102\t21.3\t2024-01-01T00:00:50.000000Z\n" +
                            "101\t20.5\t2024-01-01T00:00:50.000000Z\n" +
                            "103\t19.8\t2024-01-01T00:01:00.000000Z\n" +
                            "102\t21.3\t2024-01-01T00:01:00.000000Z\n" +
                            "103\t19.8\t2024-01-01T00:01:10.000000Z\n" +
                            "102\t21.3\t2024-01-01T00:01:10.000000Z\n" +
                            "103\t19.8\t2024-01-01T00:01:20.000000Z\n" +
                            "103\t19.8\t2024-01-01T00:01:30.000000Z\n" +
                            "103\t19.8\t2024-01-01T00:01:40.000000Z\n",
                    "WITH time_offsets AS (\n" +
                            "    SELECT (x-1) * 10 AS sec_offs, (x-1) * 10_000_000 AS usec_offs\n" +
                            "    FROM long_sequence(6)\n" +
                            ")\n" +
                            "SELECT sensor_id, temperature, reading_ts + usec_offs AS ts\n" +
                            "FROM sensor_readings CROSS JOIN time_offsets\n" +
                            "ORDER BY reading_ts + usec_offs",
                    null,
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testSingleMasterRowLargeSequence() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, '1970-01-01T00:00:00.000000Z')");

            // Test with 1201 rows to verify performance and correctness with larger sequences
            String sql = "WITH offsets AS (\n" +
                    "    SELECT x-601 AS sec_offs, 1_000_000 * (x-601) usec_offs\n" +
                    "    FROM long_sequence(1201)\n" +
                    ")\n" +
                    "SELECT id, order_ts + usec_offs AS ts\n" +
                    "FROM orders CROSS JOIN offsets\n" +
                    "ORDER BY order_ts + usec_offs";

            assertQueryNoLeakCheck(
                    "count\n" +
                            "1201\n",
                    "SELECT count(*) FROM (" + sql + ")",
                    null,
                    false,
                    true
            );

            // Verify timestamps are in sorted order
            assertQueryNoLeakCheck(
                    "id\tts\n" +
                            "1\t1969-12-31T23:50:00.000000Z\n",
                    "SELECT id, ts FROM (" + sql + ") LIMIT 1",
                    null,
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    "id\tts\n" +
                            "1\t1970-01-01T00:10:00.000000Z\n",
                    "SELECT id, ts FROM (" + sql + ") LIMIT -1",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testSingleMasterRowSmallSequence() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, '1970-01-01T00:00:00.000000Z')");

            assertQueryNoLeakCheck(
                    "id\tts\n" +
                            "1\t1970-01-01T00:00:00.000000Z\n" +
                            "1\t1970-01-01T00:00:01.000000Z\n" +
                            "1\t1970-01-01T00:00:02.000000Z\n",
                    "WITH offsets AS (\n" +
                            "    SELECT x-1 AS sec_offs, 1_000_000 * (x-1) AS usec_offs\n" +
                            "    FROM long_sequence(3)\n" +
                            ")\n" +
                            "SELECT id, order_ts + usec_offs AS ts\n" +
                            "FROM orders CROSS JOIN offsets\n" +
                            "ORDER BY order_ts + usec_offs",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testSingleSlaveCursorRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, '1970-01-01T00:00:00.000000Z')");
            execute("INSERT INTO orders VALUES (2, '1970-01-01T00:00:05.000000Z')");
            execute("INSERT INTO orders VALUES (3, '1970-01-01T00:00:10.000000Z')");

            assertQueryNoLeakCheck(
                    "id\tts\n" +
                            "1\t1970-01-01T00:00:00.000000Z\n" +
                            "2\t1970-01-01T00:00:05.000000Z\n" +
                            "3\t1970-01-01T00:00:10.000000Z\n",
                    "WITH offsets AS (\n" +
                            "    SELECT x-1 AS sec_offs, 1_000_000 * (x-1) usec_offs\n" +
                            "    FROM long_sequence(1)\n" +
                            ")\n" +
                            "SELECT id, order_ts + usec_offs AS ts\n" +
                            "FROM orders CROSS JOIN offsets\n" +
                            "ORDER BY order_ts + usec_offs",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testToTopAndReexecution() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, '1970-01-01T00:00:00.000000Z')");
            execute("INSERT INTO orders VALUES (2, '1970-01-01T00:00:01.000000Z')");

            String sql = "WITH offsets AS (\n" +
                    "    SELECT x-1 AS sec_offs, 1_000_000 * (x-1) usec_offs\n" +
                    "    FROM long_sequence(3)\n" +
                    ")\n" +
                    "SELECT id, order_ts + usec_offs AS ts\n" +
                    "FROM orders CROSS JOIN offsets\n" +
                    "ORDER BY order_ts + usec_offs";

            String expected = "id\tts\n" +
                    "1\t1970-01-01T00:00:00.000000Z\n" +
                    "1\t1970-01-01T00:00:01.000000Z\n" +
                    "2\t1970-01-01T00:00:01.000000Z\n" +
                    "1\t1970-01-01T00:00:02.000000Z\n" +
                    "2\t1970-01-01T00:00:02.000000Z\n" +
                    "2\t1970-01-01T00:00:03.000000Z\n";

            // Execute multiple times to test cursor reuse
            assertQueryNoLeakCheck(expected, sql, null, "ts", true, false);
            assertQueryNoLeakCheck(expected, sql, null, "ts", true, false);
            assertQueryNoLeakCheck(expected, sql, null, "ts", true, false);
        });
    }

    @Test
    public void testVeryCloseTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            // Timestamps 1 microsecond apart
            execute("INSERT INTO orders VALUES (1, 0)");
            execute("INSERT INTO orders VALUES (2, 1)");
            execute("INSERT INTO orders VALUES (3, 2)");

            assertQueryNoLeakCheck(
                    "id\tts\n" +
                            "1\t1970-01-01T00:00:00.000000Z\n" +
                            "2\t1970-01-01T00:00:00.000001Z\n" +
                            "3\t1970-01-01T00:00:00.000002Z\n" +
                            "1\t1970-01-01T00:00:01.000000Z\n" +
                            "2\t1970-01-01T00:00:01.000001Z\n" +
                            "3\t1970-01-01T00:00:01.000002Z\n",
                    "WITH offsets AS (\n" +
                            "    SELECT x-1 AS sec_offs, 1_000_000 * (x-1) usec_offs\n" +
                            "    FROM long_sequence(2)\n" +
                            ")\n" +
                            "SELECT id, order_ts + usec_offs AS ts\n" +
                            "FROM orders CROSS JOIN offsets\n" +
                            "ORDER BY order_ts + usec_offs",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testWithAdditionalColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, customer STRING, amount DOUBLE, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, 'Alice', 100.50, '1970-01-01T00:00:00.000000Z')");
            execute("INSERT INTO orders VALUES (2, 'Bob', 250.75, '1970-01-01T00:00:05.000000Z')");

            assertQueryNoLeakCheck(
                    "id\tcustomer\tamount\tts\n" +
                            "1\tAlice\t100.5\t1970-01-01T00:00:00.000000Z\n" +
                            "1\tAlice\t100.5\t1970-01-01T00:00:01.000000Z\n" +
                            "1\tAlice\t100.5\t1970-01-01T00:00:02.000000Z\n" +
                            "2\tBob\t250.75\t1970-01-01T00:00:05.000000Z\n" +
                            "2\tBob\t250.75\t1970-01-01T00:00:06.000000Z\n" +
                            "2\tBob\t250.75\t1970-01-01T00:00:07.000000Z\n",
                    "WITH offsets AS (\n" +
                            "    SELECT x-1 AS sec_offs, 1_000_000 * (x-1) usec_offs\n" +
                            "    FROM long_sequence(3)\n" +
                            ")\n" +
                            "SELECT id, customer, amount, order_ts + usec_offs AS ts\n" +
                            "FROM orders CROSS JOIN offsets\n" +
                            "ORDER BY order_ts + usec_offs",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testWithSlaveColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, '1970-01-01T00:00:00.000000Z')");
            execute("INSERT INTO orders VALUES (2, '1970-01-01T00:00:05.000000Z')");

            assertQueryNoLeakCheck(
                    "id\tsec_offs\tts\n" +
                            "1\t0\t1970-01-01T00:00:00.000000Z\n" +
                            "1\t1\t1970-01-01T00:00:01.000000Z\n" +
                            "1\t2\t1970-01-01T00:00:02.000000Z\n" +
                            "2\t0\t1970-01-01T00:00:05.000000Z\n" +
                            "2\t1\t1970-01-01T00:00:06.000000Z\n" +
                            "2\t2\t1970-01-01T00:00:07.000000Z\n",
                    "WITH offsets AS (\n" +
                            "    SELECT x-1 AS sec_offs, 1_000_000 * (x-1) usec_offs\n" +
                            "    FROM long_sequence(3)\n" +
                            ")\n" +
                            "SELECT id, sec_offs, order_ts + usec_offs AS ts\n" +
                            "FROM orders CROSS JOIN offsets\n" +
                            "ORDER BY order_ts + usec_offs",
                    null,
                    false,
                    true
            );
        });
    }
}
