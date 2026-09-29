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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

/**
 * Covers random access ({@code recordAt}) over a symbol index scan on a table whose
 * leading partitions are Parquet. Each Parquet row group becomes its own page frame,
 * so the row ids the index scan hands out must address the same frames that
 * {@code recordAt} navigates to, including when frames without matches come first.
 */
public class ParquetIndexScanRandomAccessTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // 12 rows per DAY partition split into three 4-row row groups, so every
        // Parquet partition yields three page frames
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
    }

    @Test
    public void testManyMatchesAcrossPartitionsWithNulls() throws Exception {
        assertIndexScan(
                "CASE WHEN x % 9 = 0 THEN 'x' ELSE NULL END",
                false,
                """
                        id\tts
                        9\t2024-01-01T16:00:00.000000Z
                        18\t2024-01-02T10:00:00.000000Z
                        27\t2024-01-03T04:00:00.000000Z
                        36\t2024-01-03T22:00:00.000000Z
                        """
        );
    }

    @Test
    public void testOrderByLimitOverIndexScan() throws Exception {
        assertMemoryLeak(() -> {
            createLongTable();
            assertQuery("SELECT id, ts FROM pq WHERE sym = 'x' ORDER BY id DESC LIMIT 2")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tts
                            7\t2024-01-01T12:00:00.000000Z
                            6\t2024-01-01T10:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testOrderByOverIndexScan() throws Exception {
        assertMemoryLeak(() -> {
            createLongTable();
            assertQuery("SELECT id, ts FROM pq WHERE sym = 'x' ORDER BY id DESC")
                    .noLeakCheck()
                    .returns("""
                            id\tts
                            7\t2024-01-01T12:00:00.000000Z
                            6\t2024-01-01T10:00:00.000000Z
                            5\t2024-01-01T08:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testSingleMatchNoNulls() throws Exception {
        assertIndexScan(
                "CASE WHEN x = 6 THEN 'x' ELSE 'y' END",
                false,
                """
                        id\tts
                        6\t2024-01-01T10:00:00.000000Z
                        """
        );
    }

    @Test
    public void testSingleMatchNoNullsNativeAfter() throws Exception {
        assertIndexScan(
                "CASE WHEN x = 6 THEN 'x' ELSE 'y' END",
                true,
                """
                        id\tts
                        6\t2024-01-01T10:00:00.000000Z
                        """
        );
    }

    @Test
    public void testSingleMatchWithNulls() throws Exception {
        assertIndexScan(
                "CASE WHEN x = 6 THEN 'x' ELSE NULL END",
                false,
                """
                        id\tts
                        6\t2024-01-01T10:00:00.000000Z
                        """
        );
    }

    @Test
    public void testSingleMatchWithNullsNativeAfter() throws Exception {
        assertIndexScan(
                "CASE WHEN x = 6 THEN 'x' ELSE NULL END",
                true,
                """
                        id\tts
                        6\t2024-01-01T10:00:00.000000Z
                        """
        );
    }

    @Test
    public void testTwoMatchesNoNulls() throws Exception {
        assertIndexScan(
                "CASE WHEN x IN (6, 7) THEN 'x' ELSE 'y' END",
                false,
                """
                        id\tts
                        6\t2024-01-01T10:00:00.000000Z
                        7\t2024-01-01T12:00:00.000000Z
                        """
        );
    }

    @Test
    public void testTwoMatchesWithNulls() throws Exception {
        assertIndexScan(
                "CASE WHEN x IN (6, 7) THEN 'x' ELSE NULL END",
                false,
                """
                        id\tts
                        6\t2024-01-01T10:00:00.000000Z
                        7\t2024-01-01T12:00:00.000000Z
                        """
        );
    }

    @Test
    public void testTwoMatchesWithNullsNativeAfter() throws Exception {
        assertIndexScan(
                "CASE WHEN x IN (6, 7) THEN 'x' ELSE NULL END",
                true,
                """
                        id\tts
                        6\t2024-01-01T10:00:00.000000Z
                        7\t2024-01-01T12:00:00.000000Z
                        """
        );
    }

    // The sort re-reads each row through recordAt() after the index scan has
    // decoded every later Parquet frame. Sorts use the SCATTERED decode hint, which
    // caches up to 256 frames, so the table needs more than 256 Parquet frames after
    // the matches before the scan recycles the buffer the scan's record still aliases.
    private static void createLongTable() throws Exception {
        execute("CREATE TABLE pq (id INT, ts TIMESTAMP, sym SYMBOL INDEX) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute(
                """
                        INSERT INTO pq
                        SELECT
                            x::INT,
                            '2024-01-01'::TIMESTAMP + (x - 1) * 7_200_000_000L,
                            CASE WHEN x IN (5, 6, 7) THEN 'x' ELSE NULL END
                        FROM long_sequence(1_200)
                        """
        );
        drainWalQueue();
        execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2024-04-01'");
        drainWalQueue();
    }

    private static void createTable(String symbolExpr, boolean nativeAfter) throws Exception {
        execute("CREATE TABLE pq (id INT, ts TIMESTAMP, sym SYMBOL INDEX) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute(
                "INSERT INTO pq SELECT x::INT, '2024-01-01'::TIMESTAMP + (x - 1) * 7_200_000_000L, "
                        + symbolExpr + " FROM long_sequence(36)"
        );
        if (nativeAfter) {
            execute("INSERT INTO pq (id, ts) VALUES (99, '2024-01-04T01:00')");
        }
        drainWalQueue();
        execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '" + (nativeAfter ? "2024-01-04" : "2024-01-03") + "'");
        drainWalQueue();
    }

    private void assertIndexScan(String symbolExpr, boolean nativeAfter, String expected) throws Exception {
        assertMemoryLeak(() -> {
            createTable(symbolExpr, nativeAfter);
            assertQuery("SELECT id, ts FROM pq WHERE sym = 'x'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
        });
    }
}
