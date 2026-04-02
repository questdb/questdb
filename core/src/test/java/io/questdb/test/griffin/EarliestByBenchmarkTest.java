/*+*****************************************************************************
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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Benchmark comparing EARLIEST ON against alternative SQL approaches for
 * finding the first row per partition key. Run manually by removing @Ignore.
 */
public class EarliestByBenchmarkTest extends AbstractCairoTest {

    private static final int WARMUP_ITERATIONS = 3;
    private static final int MEASURED_ITERATIONS = 10;

    @Test
    @Ignore("Manual benchmark — remove @Ignore to run")
    public void testEarliestOnVsAlternatives() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with 1M rows, 1000 distinct symbols, partitioned by day
            // Data spans ~41 days (1M rows at 1 row per 3.6 seconds)
            execute("""
                    CREATE TABLE sensor_data AS (
                        SELECT
                            rnd_symbol(1000, 4, 8, 0) device_id,
                            rnd_double() * 100 temperature,
                            rnd_double() * 50 humidity,
                            timestamp_sequence('2024-01-01', 3_600_000) ts
                        FROM long_sequence(1_000_000)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);

            // Also create an indexed version
            execute("""
                    CREATE TABLE sensor_data_indexed AS (
                        SELECT
                            rnd_symbol(1000, 4, 8, 0) device_id,
                            rnd_double() * 100 temperature,
                            rnd_double() * 50 humidity,
                            timestamp_sequence('2024-01-01', 3_600_000) ts
                        FROM long_sequence(1_000_000)
                    ), INDEX(device_id) TIMESTAMP(ts) PARTITION BY DAY
                    """);

            System.out.println("\n========== EARLIEST ON Benchmark ==========");
            System.out.println("Table: 1M rows, 1000 distinct symbols, ~41 day partitions\n");

            // --- Approach 1: EARLIEST ON (direct, non-indexed) ---
            long earliestOnTime = benchmarkQuery(
                    "EARLIEST ON (non-indexed)",
                    "SELECT * FROM sensor_data EARLIEST ON ts PARTITION BY device_id"
            );

            // --- Approach 2: EARLIEST ON (indexed) ---
            long earliestOnIndexedTime = benchmarkQuery(
                    "EARLIEST ON (indexed)",
                    "SELECT * FROM sensor_data_indexed EARLIEST ON ts PARTITION BY device_id"
            );

            // --- Approach 3: GROUP BY + JOIN ---
            long groupByJoinTime = benchmarkQuery(
                    "GROUP BY + JOIN",
                    """
                    SELECT d.*
                    FROM sensor_data d
                    JOIN (
                        SELECT device_id, min(ts) AS min_ts
                        FROM sensor_data
                        GROUP BY device_id
                    ) g ON d.device_id = g.device_id AND d.ts = g.min_ts
                    """
            );

            // --- Approach 4: EARLIEST ON with WHERE filter ---
            long earliestOnFilteredTime = benchmarkQuery(
                    "EARLIEST ON + WHERE",
                    "SELECT * FROM sensor_data WHERE temperature > 50 EARLIEST ON ts PARTITION BY device_id"
            );

            // --- Approach 5: GROUP BY + JOIN with WHERE filter ---
            long groupByJoinFilteredTime = benchmarkQuery(
                    "GROUP BY + JOIN + WHERE",
                    """
                    SELECT d.*
                    FROM sensor_data d
                    JOIN (
                        SELECT device_id, min(ts) AS min_ts
                        FROM sensor_data
                        WHERE temperature > 50
                        GROUP BY device_id
                    ) g ON d.device_id = g.device_id AND d.ts = g.min_ts
                    WHERE d.temperature > 50
                    """
            );

            // --- Approach 6: EARLIEST ON with interval filter ---
            long earliestOnIntervalTime = benchmarkQuery(
                    "EARLIEST ON + interval",
                    "SELECT * FROM sensor_data WHERE ts IN '2024-01-15' EARLIEST ON ts PARTITION BY device_id"
            );

            // --- Approach 7: GROUP BY + JOIN with interval filter ---
            long groupByJoinIntervalTime = benchmarkQuery(
                    "GROUP BY + JOIN + interval",
                    """
                    SELECT d.*
                    FROM sensor_data d
                    JOIN (
                        SELECT device_id, min(ts) AS min_ts
                        FROM sensor_data
                        WHERE ts IN '2024-01-15'
                        GROUP BY device_id
                    ) g ON d.device_id = g.device_id AND d.ts = g.min_ts
                    WHERE d.ts IN '2024-01-15'
                    """
            );

            System.out.println("\n========== Summary ==========");
            System.out.printf("%-35s %8s %8s%n", "Query", "Avg (us)", "Speedup");
            System.out.println("-".repeat(55));
            printSummaryRow("EARLIEST ON (non-indexed)", earliestOnTime, groupByJoinTime);
            printSummaryRow("EARLIEST ON (indexed)", earliestOnIndexedTime, groupByJoinTime);
            printSummaryRow("GROUP BY + JOIN", groupByJoinTime, groupByJoinTime);
            System.out.println();
            printSummaryRow("EARLIEST ON + WHERE", earliestOnFilteredTime, groupByJoinFilteredTime);
            printSummaryRow("GROUP BY + JOIN + WHERE", groupByJoinFilteredTime, groupByJoinFilteredTime);
            System.out.println();
            printSummaryRow("EARLIEST ON + interval", earliestOnIntervalTime, groupByJoinIntervalTime);
            printSummaryRow("GROUP BY + JOIN + interval", groupByJoinIntervalTime, groupByJoinIntervalTime);
            System.out.println();
        });
    }

    private long benchmarkQuery(String label, String sql) throws Exception {
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            drainCursor(sql);
        }

        // Measured
        long totalNanos = 0;
        int rowCount = 0;
        for (int i = 0; i < MEASURED_ITERATIONS; i++) {
            long start = System.nanoTime();
            rowCount = drainCursor(sql);
            totalNanos += System.nanoTime() - start;
        }

        long avgMicros = totalNanos / MEASURED_ITERATIONS / 1000;
        System.out.printf("  %-35s %8d us  (%d rows)%n", label, avgMicros, rowCount);
        return avgMicros;
    }

    private int drainCursor(String sql) throws Exception {
        int count = 0;
        try (RecordCursorFactory factory = select(sql);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            while (cursor.hasNext()) {
                count++;
            }
        }
        return count;
    }

    private void printSummaryRow(String label, long time, long baseline) {
        String speedup = time < baseline
                ? String.format("%.1fx", (double) baseline / time)
                : time == baseline ? "1.0x (baseline)" : String.format("%.1fx slower", (double) time / baseline);
        System.out.printf("%-35s %8d %s%n", label, time, speedup);
    }
}
