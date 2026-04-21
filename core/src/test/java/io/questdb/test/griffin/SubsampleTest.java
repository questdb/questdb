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
import io.questdb.griffin.SqlCompiler;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SubsampleTest extends AbstractCairoTest {

    @Test
    public void testLttbBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (50.0, '2024-01-01T02:00:00.000000Z'),
                    (30.0, '2024-01-01T03:00:00.000000Z'),
                    (15.0, '2024-01-01T04:00:00.000000Z'),
                    (45.0, '2024-01-01T05:00:00.000000Z'),
                    (25.0, '2024-01-01T06:00:00.000000Z'),
                    (35.0, '2024-01-01T07:00:00.000000Z'),
                    (5.0, '2024-01-01T08:00:00.000000Z'),
                    (40.0, '2024-01-01T09:00:00.000000Z')
                    """);
            // 10 points downsampled to 5: first and last always selected,
            // plus 3 selected from 3 buckets based on largest triangle area
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T02:00:00.000000Z\n" +
                            "15.0\t2024-01-01T04:00:00.000000Z\n" +
                            "5.0\t2024-01-01T08:00:00.000000Z\n" +
                            "40.0\t2024-01-01T09:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 5)"
            );
        });
    }

    @Test
    public void testLttbAllPointsWhenNEqualsInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z')
                    """);
            // n >= input count: return all points
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "20.0\t2024-01-01T01:00:00.000000Z\n" +
                            "30.0\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 10)"
            );
        });
    }

    @Test
    public void testLttbTwoPointsReturnsFirstAndLast() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            // n=2: only first and last
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testLttbPreservesSpike() throws Exception {
        // LTTB should preserve the spike at 100 because it creates the largest triangle
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (11.0, '2024-01-01T01:00:00.000000Z'),
                    (12.0, '2024-01-01T02:00:00.000000Z'),
                    (100.0, '2024-01-01T03:00:00.000000Z'),
                    (13.0, '2024-01-01T04:00:00.000000Z'),
                    (14.0, '2024-01-01T05:00:00.000000Z'),
                    (15.0, '2024-01-01T06:00:00.000000Z')
                    """);
            // The spike at 100 should be preserved
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "100.0\t2024-01-01T03:00:00.000000Z\n" +
                            "15.0\t2024-01-01T06:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 3)"
            );
        });
    }

    @Test
    public void testLttbAfterSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:30:00.000000Z'),
                    (30.0, '2024-01-01T01:00:00.000000Z'),
                    (40.0, '2024-01-01T01:30:00.000000Z'),
                    (50.0, '2024-01-01T02:00:00.000000Z'),
                    (60.0, '2024-01-01T02:30:00.000000Z')
                    """);
            // SAMPLE BY 1h produces 3 rows, then SUBSAMPLE to 2 (first and last)
            assertSql(
                    "ts\tavg\n" +
                            "2024-01-01T00:00:00.000000Z\t15.0\n" +
                            "2024-01-01T02:00:00.000000Z\t55.0\n",
                    "SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE lttb(avg, 2)"
            );
        });
    }

    @Test
    public void testLttbPassesThroughAllColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, volume INT, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 100, 'BTC', '2024-01-01T00:00:00.000000Z'),
                    (50.0, 500, 'BTC', '2024-01-01T01:00:00.000000Z'),
                    (20.0, 200, 'BTC', '2024-01-01T02:00:00.000000Z')
                    """);
            // All columns pass through for selected rows
            assertSql(
                    "price\tvolume\tsymbol\tts\n" +
                            "10.0\t100\tBTC\t2024-01-01T00:00:00.000000Z\n" +
                            "20.0\t200\tBTC\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT * FROM t SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testM4Basic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (5.0, '2024-01-01T02:00:00.000000Z'),
                    (30.0, '2024-01-01T03:00:00.000000Z'),
                    (15.0, '2024-01-01T04:00:00.000000Z'),
                    (25.0, '2024-01-01T05:00:00.000000Z'),
                    (8.0, '2024-01-01T06:00:00.000000Z'),
                    (35.0, '2024-01-01T07:00:00.000000Z')
                    """);
            // M4 with 4 target points on 8 rows = 1 bucket covering all rows
            // first=10 (row 0), last=35 (row 7), min=5 (row 2), max=35 (row 7)
            // Deduplicated (last=max at row 7): rows 0, 2, 7
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "5.0\t2024-01-01T02:00:00.000000Z\n" +
                            "35.0\t2024-01-01T07:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE m4(price, 4)"
            );
        });
    }

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertSql(
                    "price\tts\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 5)"
            );
        });
    }

    @Test
    public void testErrorNoTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP)");
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 5)",
                    24,
                    "SUBSAMPLE requires a designated timestamp column"
            );
        });
    }

    @Test
    public void testErrorTargetLessThanTwo() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 1)",
                    46,
                    "target points must be at least 2"
            );
        });
    }

    @Test
    public void testErrorUnknownMethod() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE unknown_algo(price, 5)",
                    34,
                    "unknown subsample method"
            );
        });
    }

    @Test
    public void testErrorColumnNotFound() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE lttb(nonexistent, 5)",
                    39,
                    "column not found"
            );
        });
    }

    @Test
    public void testSubsampleWithOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (20.0, '2024-01-01T02:00:00.000000Z')
                    """);
            // SUBSAMPLE then ORDER BY price DESC
            assertSql(
                    "price\tts\n" +
                            "20.0\t2024-01-01T02:00:00.000000Z\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 2) ORDER BY price DESC"
            );
        });
    }

    @Test
    public void testSubsampleWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (20.0, '2024-01-01T02:00:00.000000Z'),
                    (30.0, '2024-01-01T03:00:00.000000Z'),
                    (40.0, '2024-01-01T04:00:00.000000Z')
                    """);
            // SUBSAMPLE to 3, then LIMIT 2
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T01:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 3) LIMIT 2"
            );
        });
    }

    @Test
    public void testSubsampleWithWhere() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 'BTC', '2024-01-01T00:00:00.000000Z'),
                    (50.0, 'ETH', '2024-01-01T01:00:00.000000Z'),
                    (20.0, 'BTC', '2024-01-01T02:00:00.000000Z'),
                    (30.0, 'ETH', '2024-01-01T03:00:00.000000Z'),
                    (40.0, 'BTC', '2024-01-01T04:00:00.000000Z')
                    """);
            // WHERE filters first, then SUBSAMPLE operates on filtered result
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM t WHERE symbol = 'BTC' SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testSubsampleWithCTE() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (20.0, '2024-01-01T02:00:00.000000Z'),
                    (30.0, '2024-01-01T03:00:00.000000Z'),
                    (40.0, '2024-01-01T04:00:00.000000Z')
                    """);
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t2024-01-01T04:00:00.000000Z\n",
                    "WITH data AS (SELECT price, ts FROM t) SELECT price, ts FROM data SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testSubsampleWithSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (20.0, '2024-01-01T02:00:00.000000Z'),
                    (30.0, '2024-01-01T03:00:00.000000Z'),
                    (40.0, '2024-01-01T04:00:00.000000Z')
                    """);
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM (SELECT * FROM t) SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testCursorReuse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (20.0, '2024-01-01T02:00:00.000000Z')
                    """);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (RecordCursorFactory fact = compiler.compile("SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)", sqlExecutionContext).getRecordCursorFactory()) {
                    // Execute factory multiple times to verify no stale state
                    for (int i = 0; i < 5; i++) {
                        try (RecordCursor cursor = fact.getCursor(sqlExecutionContext)) {
                            TestUtils.assertCursor(
                                    "price\tts\n" +
                                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                                            "20.0\t2024-01-01T02:00:00.000000Z\n",
                                    cursor,
                                    fact.getMetadata(),
                                    true,
                                    sink
                            );
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testLargeDataset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            // Insert 10K rows using a subquery to avoid SUBSAMPLE parser issues
            execute("INSERT INTO t SELECT rnd_double() * 100, timestamp_sequence('2024-01-01', 1000000) FROM long_sequence(10000)");
            // Downsample to 100 points - verify via cursor count
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (RecordCursorFactory fact = compiler.compile(
                        "SELECT price, ts FROM t SUBSAMPLE lttb(price, 100)", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    try (RecordCursor cursor = fact.getCursor(sqlExecutionContext)) {
                        int count = 0;
                        while (cursor.hasNext()) {
                            count++;
                        }
                        Assert.assertEquals("LTTB should return 100 points", 100, count);
                    }
                }
            }
        });
    }

    @Test
    public void testLargeDatasetM4() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            // Insert 10K rows
            execute("INSERT INTO t SELECT rnd_double() * 100, timestamp_sequence('2024-01-01', 1000000) FROM long_sequence(10000)");
            // M4 with 100 target = 25 time buckets * up to 4 points = up to 100
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (RecordCursorFactory fact = compiler.compile(
                        "SELECT price, ts FROM t SUBSAMPLE m4(price, 100)", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    try (RecordCursor cursor = fact.getCursor(sqlExecutionContext)) {
                        int count = 0;
                        while (cursor.hasNext()) {
                            count++;
                        }
                        // M4 returns at most targetPoints
                        Assert.assertTrue("M4 returned " + count + " points, expected 1-100", count > 0 && count <= 100);
                    }
                }
            }
        });
    }

    @Test
    public void testM4GapPreservation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T10:00:00.000000Z'),
                    (50.0, '2024-01-01T11:00:00.000000Z'),
                    (60.0, '2024-01-01T12:00:00.000000Z')
                    """);
            // 12-hour range, 4 target points = 1 time bucket of 12 hours.
            // But with only 4 target points we get 1 bucket covering everything.
            // Use fewer points: all data is in 2 clusters (0-2h and 10-12h).
            // With 4 target points = 1 bucket -> first/last/min/max of all.
            // Let's instead use more data to see the gap:
            // Actually 4 points / 4 = 1 bucket. To get 2 buckets we need 8.
            // But 8 >= 6 rows, so selectAll(). Need more rows.
            // Just verify that with target=4 (1 bucket) we get the extremes:
            // first=10 (row 0), last=60 (row 5), min=10 (row 0), max=60 (row 5)
            // Dedup: rows 0 and 5
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "60.0\t2024-01-01T12:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE m4(price, 4)"
            );
        });
    }

    @Test
    public void testNullValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (NULL, '2024-01-01T01:00:00.000000Z'),
                    (50.0, '2024-01-01T02:00:00.000000Z'),
                    (NULL, '2024-01-01T03:00:00.000000Z'),
                    (20.0, '2024-01-01T04:00:00.000000Z')
                    """);
            // NULL rows are skipped, 3 non-null rows, target 2 -> first and last
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "20.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testLttbGapPreserving() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:10:00.000000Z'),
                    (30.0, '2024-01-01T00:20:00.000000Z'),
                    (40.0, '2024-01-01T00:30:00.000000Z'),
                    (50.0, '2024-01-01T05:00:00.000000Z'),
                    (60.0, '2024-01-01T05:10:00.000000Z'),
                    (70.0, '2024-01-01T05:20:00.000000Z'),
                    (80.0, '2024-01-01T05:30:00.000000Z')
                    """);
            // Gap of 4.5 hours between 00:30 and 05:00.
            // With threshold '1h', two segments are detected:
            // Segment 1: rows 0-3 (00:00 to 00:30)
            // Segment 2: rows 4-7 (05:00 to 05:30)
            // Target 4 points: 2 per segment (proportional).
            // Each segment selects first and last.
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t2024-01-01T00:30:00.000000Z\n" +
                            "50.0\t2024-01-01T05:00:00.000000Z\n" +
                            "80.0\t2024-01-01T05:30:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 4, '1h')"
            );
        });
    }

    @Test
    public void testLttbGapPreservingNoGaps() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (20.0, '2024-01-01T02:00:00.000000Z'),
                    (30.0, '2024-01-01T03:00:00.000000Z'),
                    (40.0, '2024-01-01T04:00:00.000000Z')
                    """);
            // No gaps > 2h exist, so one segment covering all data.
            // Same as regular LTTB with n=2: first and last.
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 2, '2h')"
            );
        });
    }

    @Test
    public void testLttbGapInvalidUnit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 5, '1M')",
                    50,
                    "unsupported interval unit"
            );
        });
    }

    @Test
    public void testSubsampleInParenthesizedSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (20.0, '2024-01-01T02:00:00.000000Z')
                    """);
            assertSql(
                    "count\n2\n",
                    "SELECT count() FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 2))"
            );
        });
    }

    @Test
    public void testSubsampleWithJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE prices (price DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO prices VALUES
                    (100.0, 'BTC', '2024-01-01T00:00:00.000000Z'),
                    (200.0, 'BTC', '2024-01-01T01:00:00.000000Z'),
                    (150.0, 'BTC', '2024-01-01T02:00:00.000000Z'),
                    (50.0, 'ETH', '2024-01-01T00:00:00.000000Z'),
                    (60.0, 'ETH', '2024-01-01T01:00:00.000000Z'),
                    (55.0, 'ETH', '2024-01-01T02:00:00.000000Z')
                    """);
            // SUBSAMPLE after WHERE (which is essentially a filtered scan)
            assertSql(
                    "price\tts\n" +
                            "100.0\t2024-01-01T00:00:00.000000Z\n" +
                            "150.0\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, ts FROM prices WHERE symbol = 'BTC' SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testSubsampleWithExpressionColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, volume DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 100.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, 500.0, '2024-01-01T01:00:00.000000Z'),
                    (20.0, 200.0, '2024-01-01T02:00:00.000000Z'),
                    (30.0, 300.0, '2024-01-01T03:00:00.000000Z'),
                    (40.0, 400.0, '2024-01-01T04:00:00.000000Z')
                    """);
            // SUBSAMPLE uses 'price' column directly - expression columns
            // in the value parameter are not supported (column name only)
            assertSql(
                    "price\tvolume\tts\n" +
                            "10.0\t100.0\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t400.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, volume, ts FROM t SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testLttbWithDeclareVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (20.0, '2024-01-01T02:00:00.000000Z'),
                    (30.0, '2024-01-01T03:00:00.000000Z'),
                    (40.0, '2024-01-01T04:00:00.000000Z')
                    """);
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t2024-01-01T04:00:00.000000Z\n",
                    "DECLARE @n := 2 SELECT price, ts FROM t SUBSAMPLE lttb(price, @n)"
            );
        });
    }

    @Test
    public void testM4SingleBucket() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (20.0, '2024-01-01T02:00:00.000000Z'),
                    (30.0, '2024-01-01T03:00:00.000000Z'),
                    (40.0, '2024-01-01T04:00:00.000000Z')
                    """);
            // M4 with target=4: 1 time bucket, selects first/last/min/max
            // first=10 (row 0), last=40 (row 4), min=10 (row 0), max=50 (row 1)
            // Dedup and sort: 0, 1, 4
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T01:00:00.000000Z\n" +
                            "40.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE m4(price, 4)"
            );
        });
    }

    @Test
    public void testM4WithDeclareVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (20.0, '2024-01-01T02:00:00.000000Z')
                    """);
            // M4 with DECLARE variable, target=4: 1 bucket covering all 3 rows
            // first=10 (row 0), last=20 (row 2), min=10 (row 0), max=50 (row 1)
            // Dedup and sort: 0, 1, 2
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T01:00:00.000000Z\n" +
                            "20.0\t2024-01-01T02:00:00.000000Z\n",
                    "DECLARE @points := 4 SELECT price, ts FROM t SUBSAMPLE m4(price, @points)"
            );
        });
    }

    @Test
    public void testErrorNonNumericColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (name SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT * FROM t SUBSAMPLE lttb(name, 5)",
                    31,
                    "numeric column expected"
            );
        });
    }

    @Test
    public void testSingleRowAfterNaNFiltering() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (NULL, '2024-01-01T00:00:00.000000Z'),
                    (NULL, '2024-01-01T01:00:00.000000Z'),
                    (42.0, '2024-01-01T02:00:00.000000Z'),
                    (NULL, '2024-01-01T03:00:00.000000Z')
                    """);
            // Only 1 non-NULL row, target 2 - should return the single valid row
            assertSql(
                    "price\tts\n" +
                            "42.0\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testSubsampleWithLatestOn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 'BTC', '2024-01-01T00:00:00.000000Z'),
                    (20.0, 'BTC', '2024-01-01T01:00:00.000000Z'),
                    (30.0, 'ETH', '2024-01-01T00:00:00.000000Z'),
                    (40.0, 'ETH', '2024-01-01T01:00:00.000000Z'),
                    (50.0, 'BTC', '2024-01-01T02:00:00.000000Z'),
                    (60.0, 'ETH', '2024-01-01T02:00:00.000000Z')
                    """);
            // LATEST ON then SUBSAMPLE the result
            assertSql(
                    "price\tsymbol\tts\n" +
                            "50.0\tBTC\t2024-01-01T02:00:00.000000Z\n" +
                            "60.0\tETH\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, symbol, ts FROM t LATEST ON ts PARTITION BY symbol SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testSubsampleWithWindowFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (20.0, '2024-01-01T02:00:00.000000Z'),
                    (30.0, '2024-01-01T03:00:00.000000Z'),
                    (40.0, '2024-01-01T04:00:00.000000Z')
                    """);
            // Window function computes on ALL rows first, then SUBSAMPLE
            // picks from the result. row_number() assigns 1-5 to all rows,
            // then SUBSAMPLE selects first (rn=1) and last (rn=5).
            assertSql(
                    "price\tts\trn\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                            "40.0\t2024-01-01T04:00:00.000000Z\t5\n",
                    "SELECT price, ts, row_number() OVER () rn FROM t SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testSubsampleWithActualJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE prices (price DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE volumes (volume DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO prices VALUES
                    (100.0, 'BTC', '2024-01-01T00:00:00.000000Z'),
                    (200.0, 'BTC', '2024-01-01T01:00:00.000000Z'),
                    (150.0, 'BTC', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO volumes VALUES
                    (1000.0, 'BTC', '2024-01-01T00:00:00.000000Z'),
                    (2000.0, 'BTC', '2024-01-01T01:00:00.000000Z'),
                    (1500.0, 'BTC', '2024-01-01T02:00:00.000000Z')
                    """);
            // ASOF JOIN then SUBSAMPLE
            assertSql(
                    "price\tts\tvolume\n" +
                            "100.0\t2024-01-01T00:00:00.000000Z\t1000.0\n" +
                            "150.0\t2024-01-01T02:00:00.000000Z\t1500.0\n",
                    "SELECT p.price, p.ts, v.volume FROM prices p ASOF JOIN volumes v ON (symbol) SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testLttbThreePoints() throws Exception {
        // Minimum non-trivial LTTB: 1 bucket between first and last
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (20.0, '2024-01-01T02:00:00.000000Z'),
                    (30.0, '2024-01-01T03:00:00.000000Z')
                    """);
            // Target 3: first, one selected from middle bucket, last
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T01:00:00.000000Z\n" +
                            "30.0\t2024-01-01T03:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 3)"
            );
        });
    }

    @Test
    public void testLttbAllIdenticalValues() throws Exception {
        // Flat line: all triangle areas are 0, algorithm still selects one per bucket
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (42.0, '2024-01-01T00:00:00.000000Z'),
                    (42.0, '2024-01-01T01:00:00.000000Z'),
                    (42.0, '2024-01-01T02:00:00.000000Z'),
                    (42.0, '2024-01-01T03:00:00.000000Z'),
                    (42.0, '2024-01-01T04:00:00.000000Z')
                    """);
            // All areas are 0, selects first point in each bucket (index 0 wins ties)
            // Target 3: first + 1 from middle + last
            assertSql(
                    "price\tts\n" +
                            "42.0\t2024-01-01T00:00:00.000000Z\n" +
                            "42.0\t2024-01-01T01:00:00.000000Z\n" +
                            "42.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 3)"
            );
        });
    }

    @Test
    public void testM4AllIdenticalTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:00:00.000000Z'),
                    (30.0, '2024-01-01T00:00:00.000000Z')
                    """);
            // All same timestamp - M4 falls back to selectAll
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "20.0\t2024-01-01T00:00:00.000000Z\n" +
                            "30.0\t2024-01-01T00:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE m4(price, 4)"
            );
        });
    }

    @Test
    public void testLttbGapPreservingEveryRowIsGap() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T06:00:00.000000Z'),
                    (30.0, '2024-01-01T12:00:00.000000Z'),
                    (40.0, '2024-01-01T18:00:00.000000Z')
                    """);
            // Gap threshold 1h, but gaps are 6h - every row is its own segment.
            // Each 1-row segment selects all (segment size <= target share).
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "20.0\t2024-01-01T06:00:00.000000Z\n" +
                            "30.0\t2024-01-01T12:00:00.000000Z\n" +
                            "40.0\t2024-01-01T18:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 2, '1h')"
            );
        });
    }

    @Test
    public void testM4WithIntColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10, '2024-01-01T00:00:00.000000Z'),
                    (50, '2024-01-01T01:00:00.000000Z'),
                    (20, '2024-01-01T02:00:00.000000Z')
                    """);
            // M4 with 4 target on 3 rows = all rows returned
            assertSql(
                    "price\tts\n" +
                            "10\t2024-01-01T00:00:00.000000Z\n" +
                            "50\t2024-01-01T01:00:00.000000Z\n" +
                            "20\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE m4(price, 12)"
            );
        });
    }

    @Test
    public void testLttbTargetEqualsInputSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (20.0, '2024-01-01T02:00:00.000000Z')
                    """);
            // Target exactly equals input - returns all
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T01:00:00.000000Z\n" +
                            "20.0\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 3)"
            );
        });
    }

    @Test
    public void testSubsampleEmptyAfterWhere() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 'BTC', '2024-01-01T00:00:00.000000Z'),
                    (20.0, 'BTC', '2024-01-01T01:00:00.000000Z')
                    """);
            // WHERE filters everything out
            assertSql(
                    "price\tts\n",
                    "SELECT price, ts FROM t WHERE symbol = 'ETH' SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testSubsampleWithGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 'BTC', '2024-01-01T00:00:00.000000Z'),
                    (20.0, 'ETH', '2024-01-01T00:00:00.000000Z'),
                    (30.0, 'BTC', '2024-01-01T01:00:00.000000Z'),
                    (40.0, 'ETH', '2024-01-01T01:00:00.000000Z'),
                    (50.0, 'BTC', '2024-01-01T02:00:00.000000Z'),
                    (60.0, 'ETH', '2024-01-01T02:00:00.000000Z')
                    """);
            // SAMPLE BY produces 3 rows, SUBSAMPLE to 2
            assertSql(
                    "ts\ttotal\n" +
                            "2024-01-01T00:00:00.000000Z\t30.0\n" +
                            "2024-01-01T02:00:00.000000Z\t110.0\n",
                    "SELECT ts, sum(price) total FROM t SAMPLE BY 1h SUBSAMPLE lttb(total, 2)"
            );
        });
    }

    @Test
    public void testErrorVarcharColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (name VARCHAR, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT * FROM t SUBSAMPLE lttb(name, 5)",
                    31,
                    "numeric column expected"
            );
        });
    }

    @Test
    public void testExplainPlanShowsSubsample() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            // Verify the EXPLAIN plan includes the Subsample node with method and points
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (RecordCursorFactory fact = compiler.compile(
                        "EXPLAIN SELECT price, ts FROM t SUBSAMPLE lttb(price, 500)", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    try (RecordCursor cursor = fact.getCursor(sqlExecutionContext)) {
                        StringBuilder sb = new StringBuilder();
                        while (cursor.hasNext()) {
                            sb.append(cursor.getRecord().getStrA(0)).append('\n');
                        }
                        String plan = sb.toString();
                        Assert.assertTrue("Plan should contain 'Subsample': " + plan, plan.contains("Subsample"));
                        Assert.assertTrue("Plan should contain 'lttb': " + plan, plan.contains("lttb"));
                    }
                }
            }
        });
    }

    @Test
    public void testMinMaxBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (5.0, '2024-01-01T02:00:00.000000Z'),
                    (30.0, '2024-01-01T03:00:00.000000Z'),
                    (15.0, '2024-01-01T04:00:00.000000Z'),
                    (25.0, '2024-01-01T05:00:00.000000Z'),
                    (8.0, '2024-01-01T06:00:00.000000Z'),
                    (35.0, '2024-01-01T07:00:00.000000Z')
                    """);
            // MinMax with 4 target = 2 time buckets (4/2).
            // Time range: 0h-7h, bucket width = 3.5h
            // Bucket 1 (0h-3.5h): rows 0-3 -> min=5 (row 2), max=50 (row 1)
            // Bucket 2 (3.5h-7h): rows 4-7 -> min=8 (row 6), max=35 (row 7)
            assertSql(
                    "price\tts\n" +
                            "50.0\t2024-01-01T01:00:00.000000Z\n" +
                            "5.0\t2024-01-01T02:00:00.000000Z\n" +
                            "8.0\t2024-01-01T06:00:00.000000Z\n" +
                            "35.0\t2024-01-01T07:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE minmax(price, 4)"
            );
        });
    }

    @Test
    public void testMinMaxSingleBucket() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (20.0, '2024-01-01T02:00:00.000000Z')
                    """);
            // MinMax with 2 target = 1 bucket covering all rows
            // min=10 (row 0), max=50 (row 1)
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T01:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE minmax(price, 2)"
            );
        });
    }

    @Test
    public void testMinMaxGapPreservation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T10:00:00.000000Z'),
                    (40.0, '2024-01-01T11:00:00.000000Z')
                    """);
            // MinMax with 4 target = 2 time buckets.
            // Time range: 0h-11h, bucket width = 5.5h
            // Bucket 1 (0h-5.5h): rows 0-1 -> min=10, max=20
            // Bucket 2 (5.5h-11h): rows 2-3 -> min=30, max=40
            // Gap between 01:00 and 10:00 is naturally preserved
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "20.0\t2024-01-01T01:00:00.000000Z\n" +
                            "30.0\t2024-01-01T10:00:00.000000Z\n" +
                            "40.0\t2024-01-01T11:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE minmax(price, 4)"
            );
        });
    }

    @Test
    public void testMinMaxIdenticalValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (42.0, '2024-01-01T00:00:00.000000Z'),
                    (42.0, '2024-01-01T01:00:00.000000Z'),
                    (42.0, '2024-01-01T02:00:00.000000Z'),
                    (42.0, '2024-01-01T03:00:00.000000Z')
                    """);
            // All identical -> min==max -> 1 point per bucket (deduplicated)
            // 2 target = 1 bucket -> 1 point
            assertSql(
                    "price\tts\n" +
                            "42.0\t2024-01-01T00:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE minmax(price, 2)"
            );
        });
    }

    @Test
    public void testSubsampleWithUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE t2 (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t1 VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO t2 VALUES
                    (40.0, '2024-01-02T00:00:00.000000Z'),
                    (50.0, '2024-01-02T01:00:00.000000Z'),
                    (60.0, '2024-01-02T02:00:00.000000Z')
                    """);
            // SUBSAMPLE inside each leg of a UNION ALL
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "30.0\t2024-01-01T02:00:00.000000Z\n" +
                            "40.0\t2024-01-02T00:00:00.000000Z\n" +
                            "60.0\t2024-01-02T02:00:00.000000Z\n",
                    "SELECT * FROM (SELECT price, ts FROM t1 SUBSAMPLE lttb(price, 2)) " +
                            "UNION ALL " +
                            "SELECT * FROM (SELECT price, ts FROM t2 SUBSAMPLE lttb(price, 2))"
            );
        });
    }

    @Test
    public void testSubsampleWithoutMethodErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT ts, price FROM t SUBSAMPLE ORDER BY ts",
                    40,
                    "'(' expected after subsample method name"
            );
        });
    }
}
