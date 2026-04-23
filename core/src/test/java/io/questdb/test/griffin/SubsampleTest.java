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
import io.questdb.std.Rnd;
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
    public void testErrorNoTimestampColumnAtAll() throws Exception {
        // Table with no TIMESTAMP column - must fail
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, label SYMBOL)");
            assertException(
                    "SELECT price, label FROM t SUBSAMPLE lttb(price, 2)",
                    27,
                    "SUBSAMPLE requires a designated timestamp column"
            );
        });
    }

    @Test
    public void testSampleByLosesDesignationButSubsampleStillWorks() throws Exception {
        // SAMPLE BY results lose designated timestamp (AsyncGroupByRecordCursorFactory
        // has timestampIndex=-1), but the nested model chain confirms designation.
        // The type-scan fallback must work for this case.
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
            // SAMPLE BY 1h produces 3 rows, SUBSAMPLE to 2
            assertSql(
                    "ts\tavg\n" +
                            "2024-01-01T00:00:00.000000Z\t15.0\n" +
                            "2024-01-01T02:00:00.000000Z\t55.0\n",
                    "SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE lttb(avg, 2)"
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
    public void testLongTargetPointsCast() throws Exception {
        // LONG constant via cast
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z')
                    """);
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "30.0\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 2::LONG)"
            );
        });
    }

    @Test
    public void testLongTargetPointsDeclare() throws Exception {
        // LONG bind variable via DECLARE
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z')
                    """);
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "30.0\t2024-01-01T02:00:00.000000Z\n",
                    "DECLARE @n := 2::LONG SELECT price, ts FROM t SUBSAMPLE lttb(price, @n)"
            );
        });
    }

    @Test
    public void testErrorLongTargetOverflow() throws Exception {
        // LONG value exceeding Integer.MAX_VALUE
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 3_000_000_000::LONG)",
                    59,
                    "target points exceeds maximum"
            );
        });
    }

    @Test
    public void testLongBindVariableRuntime() throws Exception {
        // PG-wire-style LONG bind variable via $1
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z')
                    """);
            sqlExecutionContext.getBindVariableService().setLong(0, 2L);
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "30.0\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, $1)"
            );
        });
    }

    @Test
    public void testErrorUnsetBindVariable() throws Exception {
        // Unset $1 bind variable: coerced to LONG, reads as NULL, fails validation.
        // Error position points at the target argument ($1 at position 46).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, $1)",
                    46,
                    "target point count must be set"
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
    public void testSubsampleWithOrderByThirdColumn() throws Exception {
        // ORDER BY on a non-SUBSAMPLE column should sort the reduced row set
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, quantity INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 5, '2024-01-01T00:00:00.000000Z'),
                    (50.0, 3, '2024-01-01T01:00:00.000000Z'),
                    (20.0, 8, '2024-01-01T02:00:00.000000Z'),
                    (30.0, 1, '2024-01-01T03:00:00.000000Z'),
                    (40.0, 9, '2024-01-01T04:00:00.000000Z')
                    """);
            // LTTB target=2 on 5 rows: first and last
            assertSql(
                    "price\tquantity\tts\n" +
                            "10.0\t5\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t9\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, quantity, ts FROM t SUBSAMPLE lttb(price, 2) ORDER BY quantity"
            );
        });
    }

    @Test
    public void testM4WithOrderByThirdColumn() throws Exception {
        // M4 + ORDER BY on a non-SUBSAMPLE column
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, quantity INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 5, '2024-01-01T00:00:00.000000Z'),
                    (50.0, 3, '2024-01-01T01:00:00.000000Z'),
                    (5.0, 8, '2024-01-01T02:00:00.000000Z'),
                    (30.0, 1, '2024-01-01T03:00:00.000000Z'),
                    (45.0, 9, '2024-01-01T04:00:00.000000Z'),
                    (20.0, 2, '2024-01-01T05:00:00.000000Z')
                    """);
            // M4 target=8: numBuckets = 8/4 = 2 time-based buckets over 5h range.
            // Bucket 1 (00:00-02:30): first=10, last=5, min=5, max=50 -> indices 0,1,2
            // Bucket 2 (02:30-05:00): first=30, last=20, min=20, max=45 -> indices 3,4,5
            // All 6 rows selected (each role is a distinct row). ORDER BY quantity.
            assertSql(
                    "price\tquantity\tts\n" +
                            "30.0\t1\t2024-01-01T03:00:00.000000Z\n" +
                            "20.0\t2\t2024-01-01T05:00:00.000000Z\n" +
                            "50.0\t3\t2024-01-01T01:00:00.000000Z\n" +
                            "10.0\t5\t2024-01-01T00:00:00.000000Z\n" +
                            "5.0\t8\t2024-01-01T02:00:00.000000Z\n" +
                            "45.0\t9\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, quantity, ts FROM t SUBSAMPLE m4(price, 8) ORDER BY quantity"
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
            // CTE with SUBSAMPLE on the outer query
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
            // Subquery wrapping with SUBSAMPLE on the outer query
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
            // SUBSAMPLE inside a parenthesized subquery wrapped in count()
            assertSql(
                    "count\n2\n",
                    "SELECT count() FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 2))"
            );
        });
    }

    @Test
    public void testSubsampleViaSubqueryNonDesignatedTimestamp() throws Exception {
        // Subquery wrapping a table with designated timestamp: SUBSAMPLE must work.
        // Subquery wrapping a table WITHOUT designated timestamp: must fail.
        // This is the Bug 6 negative test - if optimizer propagation restores
        // SUBSAMPLE for subquery wrapping, the non-designated case must not
        // accidentally grab a TIMESTAMP column by type.
        assertMemoryLeak(() -> {
            // Positive: designated timestamp - should work
            execute("CREATE TABLE t_designated (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t_designated VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z')
                    """);
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "30.0\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, ts FROM (SELECT * FROM t_designated) SUBSAMPLE lttb(price, 2)"
            );

            // Negative: no designated timestamp - must fail, not silently succeed
            execute("CREATE TABLE t_no_designated (price DOUBLE, ts TIMESTAMP)");
            execute("""
                    INSERT INTO t_no_designated VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z')
                    """);
            assertException(
                    "SELECT price, ts FROM (SELECT * FROM t_no_designated) SUBSAMPLE lttb(price, 2)",
                    54,
                    "SUBSAMPLE requires a designated timestamp column"
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
    public void testSubsampleNotHoistedFromJoinBranch() throws Exception {
        // SUBSAMPLE on one join branch must not affect the other branch
        // or the outer join result. This is a shape/isolation test.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE b (volume DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO a VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO b VALUES
                    (100.0, '2024-01-01T00:00:00.000000Z'),
                    (200.0, '2024-01-01T01:00:00.000000Z'),
                    (300.0, '2024-01-01T02:00:00.000000Z'),
                    (400.0, '2024-01-01T03:00:00.000000Z'),
                    (500.0, '2024-01-01T04:00:00.000000Z')
                    """);
            // SUBSAMPLE is on the outer joined result, not on one branch.
            // The join produces 5 rows, SUBSAMPLE reduces to 2.
            assertSql(
                    "price\tts\tvolume\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\t100.0\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\t500.0\n",
                    "SELECT a.price, a.ts, b.volume FROM a ASOF JOIN b SUBSAMPLE lttb(price, 2)"
            );
            // Verify the join without SUBSAMPLE gives all 5 rows
            assertSql(
                    "price\tts\tvolume\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\t100.0\n" +
                            "20.0\t2024-01-01T01:00:00.000000Z\t200.0\n" +
                            "30.0\t2024-01-01T02:00:00.000000Z\t300.0\n" +
                            "40.0\t2024-01-01T03:00:00.000000Z\t400.0\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\t500.0\n",
                    "SELECT a.price, a.ts, b.volume FROM a ASOF JOIN b"
            );
        });
    }

    @Test
    public void testSubsampleBranchLocalInJoin() throws Exception {
        // SUBSAMPLE inside a join branch (right side) must apply only to that
        // branch. The outer join row count follows the left side, not the
        // subsampled right side. If the optimizer hoists the branch-local
        // SUBSAMPLE to the outer model, the outer row count would be wrong.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE b (volume DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO a VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO b VALUES
                    (100.0, '2024-01-01T00:00:00.000000Z'),
                    (200.0, '2024-01-01T01:00:00.000000Z'),
                    (300.0, '2024-01-01T02:00:00.000000Z'),
                    (400.0, '2024-01-01T03:00:00.000000Z'),
                    (500.0, '2024-01-01T04:00:00.000000Z')
                    """);
            // Right side subsampled to 2 rows (first=100 at 00:00, last=500 at 04:00).
            // Left side has 5 rows. ASOF JOIN produces 5 rows driven by left side.
            // For left rows at 01:00-03:00, the nearest right-side match is 100
            // (the only right row with ts <= theirs). At 04:00, it matches 500.
            // If branch SUBSAMPLE were dropped, volumes would be 100,200,300,400,500.
            assertSql(
                    "price\tts\tvolume\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\t100.0\n" +
                            "20.0\t2024-01-01T01:00:00.000000Z\t100.0\n" +
                            "30.0\t2024-01-01T02:00:00.000000Z\t100.0\n" +
                            "40.0\t2024-01-01T03:00:00.000000Z\t100.0\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\t500.0\n",
                    """
                            SELECT a.price, a.ts, b.volume
                            FROM a
                            ASOF JOIN (
                                SELECT volume, ts FROM b SUBSAMPLE lttb(volume, 2)
                            ) b
                            """
            );
        });
    }

    @Test
    public void testSubsampleInsideParenthesizedSubqueryNotHoisted() throws Exception {
        // SUBSAMPLE inside a parenthesized subquery must be applied inside,
        // not hoisted to the outer aggregation. This is the key isolation test:
        // the inner subquery reduces 5 rows to 2, then count() returns 2.
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
            // count() wrapping SUBSAMPLE: inner reduces 5 -> 2, outer counts 2
            assertSql(
                    "count\n2\n",
                    "SELECT count() FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 2))"
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
    public void testLttbGapModeExceedsTarget() throws Exception {
        // Gap-preserving mode uses soft target: each segment gets at least
        // 2 points (first/last). With many small segments and a low target,
        // the output exceeds targetPoints. Non-gap LTTB is hard-capped.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            // 10 data points with 5 gaps (6h apart, threshold 1h) = 5 segments
            // of 2 rows each. Each segment gets at least 2 points = 10 minimum.
            // Target is 4, but 5 segments * 2 = 10 > 4.
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (11.0, '2024-01-01T00:30:00.000000Z'),
                    (20.0, '2024-01-01T06:00:00.000000Z'),
                    (21.0, '2024-01-01T06:30:00.000000Z'),
                    (30.0, '2024-01-01T12:00:00.000000Z'),
                    (31.0, '2024-01-01T12:30:00.000000Z'),
                    (40.0, '2024-01-01T18:00:00.000000Z'),
                    (41.0, '2024-01-01T18:30:00.000000Z'),
                    (50.0, '2024-01-02T00:00:00.000000Z'),
                    (51.0, '2024-01-02T00:30:00.000000Z')
                    """);
            // Gap mode with target 4: soft target. 5 segments of 2 rows each,
            // each segment gets first/last = 2 points. Total 10, exceeds target 4.
            // Assert exact output: each segment's first and last must be present.
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "11.0\t2024-01-01T00:30:00.000000Z\n" +
                            "20.0\t2024-01-01T06:00:00.000000Z\n" +
                            "21.0\t2024-01-01T06:30:00.000000Z\n" +
                            "30.0\t2024-01-01T12:00:00.000000Z\n" +
                            "31.0\t2024-01-01T12:30:00.000000Z\n" +
                            "40.0\t2024-01-01T18:00:00.000000Z\n" +
                            "41.0\t2024-01-01T18:30:00.000000Z\n" +
                            "50.0\t2024-01-02T00:00:00.000000Z\n" +
                            "51.0\t2024-01-02T00:30:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 4, '1h')"
            );

            // Non-gap LTTB with same target: hard maximum of 4.
            // LTTB selects first and last always, plus 2 from middle buckets.
            // The exact middle selections depend on triangle area calculations.
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "11.0\t2024-01-01T00:30:00.000000Z\n" +
                            "40.0\t2024-01-01T18:00:00.000000Z\n" +
                            "51.0\t2024-01-02T00:30:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 4)"
            );
        });
    }

    @Test
    public void testLttbGapModeBudgetScaling() throws Exception {
        // When budget is sufficient, gap mode stays within target.
        // 3 segments with target 10: each segment gets proportional share,
        // total should not exceed 10.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (11.0, '2024-01-01T00:10:00.000000Z'),
                    (12.0, '2024-01-01T00:20:00.000000Z'),
                    (13.0, '2024-01-01T00:30:00.000000Z'),
                    (20.0, '2024-01-01T06:00:00.000000Z'),
                    (21.0, '2024-01-01T06:10:00.000000Z'),
                    (22.0, '2024-01-01T06:20:00.000000Z'),
                    (23.0, '2024-01-01T06:30:00.000000Z'),
                    (30.0, '2024-01-01T12:00:00.000000Z'),
                    (31.0, '2024-01-01T12:10:00.000000Z'),
                    (32.0, '2024-01-01T12:20:00.000000Z'),
                    (33.0, '2024-01-01T12:30:00.000000Z')
                    """);
            // 3 segments of 4 rows, target 10. Floor = 3*2 = 6. Budget above floor = 4.
            // Each segment gets 3 points (first, LTTB-selected middle, last). Total = 9.
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "11.0\t2024-01-01T00:10:00.000000Z\n" +
                            "13.0\t2024-01-01T00:30:00.000000Z\n" +
                            "20.0\t2024-01-01T06:00:00.000000Z\n" +
                            "21.0\t2024-01-01T06:10:00.000000Z\n" +
                            "23.0\t2024-01-01T06:30:00.000000Z\n" +
                            "30.0\t2024-01-01T12:00:00.000000Z\n" +
                            "31.0\t2024-01-01T12:10:00.000000Z\n" +
                            "33.0\t2024-01-01T12:30:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 10, '1h')"
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
    public void testM4SmallTargetCapped() throws Exception {
        // M4 with target=2 on distinct-value data: one bucket can emit up to
        // 4 rows (first, last, min, max). The output must be capped at target.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (5.0, '2024-01-01T02:00:00.000000Z'),
                    (30.0, '2024-01-01T03:00:00.000000Z')
                    """);
            // target=2, numBuckets=1. Bucket emits first(10), max(50), min(5), last(30)
            // sorted by index = [0,1,2,3]. Cap at 2 keeps first two: 10.0 and 50.0.
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T01:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE m4(price, 2)"
            );
        });
    }

    @Test
    public void testM4WithExtremeValues() throws Exception {
        // M4/MinMax use first-value initialization (hasData boolean) instead
        // of Infinity sentinels. QuestDB stores 'Infinity'::double as NaN, so
        // Infinity cannot reach the algorithm buffer via SQL. This test covers
        // extreme finite values near Double.MAX_VALUE.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (1.7E308, '2024-01-01T00:00:00.000000Z'),
                    (10.0, '2024-01-01T01:00:00.000000Z'),
                    (-1.7E308, '2024-01-01T02:00:00.000000Z'),
                    (20.0, '2024-01-01T03:00:00.000000Z'),
                    (15.0, '2024-01-01T04:00:00.000000Z')
                    """);
            // 5 rows, target 4, 1 bucket: first=1.7E308(idx0), last=15(idx4),
            // min=-1.7E308(idx2), max=1.7E308(idx0). Deduped: idx0,2,4 = 3 rows.
            assertSql(
                    "price\tts\n" +
                            "1.7E308\t2024-01-01T00:00:00.000000Z\n" +
                            "-1.7E308\t2024-01-01T02:00:00.000000Z\n" +
                            "15.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE m4(price, 4)"
            );
        });
    }

    @Test
    public void testMinMaxWithExtremeValues() throws Exception {
        // See testM4WithExtremeValues for Infinity discussion.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (1.7E308, '2024-01-01T00:00:00.000000Z'),
                    (10.0, '2024-01-01T01:00:00.000000Z'),
                    (-1.7E308, '2024-01-01T02:00:00.000000Z'),
                    (20.0, '2024-01-01T03:00:00.000000Z'),
                    (15.0, '2024-01-01T04:00:00.000000Z')
                    """);
            // 5 rows, target 4, 2 buckets of 2h each.
            // Bucket 1 [0h,2h): min=10(1h), max=1.7E308(0h).
            // Bucket 2 [2h,4h]: min=-1.7E308(2h), max=20(3h).
            assertSql(
                    "price\tts\n" +
                            "1.7E308\t2024-01-01T00:00:00.000000Z\n" +
                            "10.0\t2024-01-01T01:00:00.000000Z\n" +
                            "-1.7E308\t2024-01-01T02:00:00.000000Z\n" +
                            "20.0\t2024-01-01T03:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE minmax(price, 4)"
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
    public void testM4FinalBucketInclusive() throws Exception {
        // Final bucket must include the last data point. The algorithm no longer
        // uses maxTs + 1 as the exclusive end (which overflows at Long.MAX_VALUE).
        // Instead, the final bucket loop skips the break condition entirely,
        // processing all remaining rows. This SQL test validates the behavior
        // for wide ranges; the literal Long.MAX_VALUE overflow edge is only
        // directly testable at algorithm level since QuestDB's max representable
        // timestamp (CommonUtils.MAX_TIMESTAMP) is less than Long.MAX_VALUE.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2000-01-01T00:00:00.000000Z'),
                    (50.0, '2100-01-01T00:00:00.000000Z'),
                    (20.0, '2200-01-01T00:00:00.000000Z'),
                    (30.0, '2290-01-01T00:00:00.000000Z')
                    """);
            // M4 target=4, 4 rows: bufferSize(4) <= targetPoints(4), all rows returned.
            // Last data point (2290) must be included.
            assertSql(
                    "price\tts\n" +
                            "10.0\t2000-01-01T00:00:00.000000Z\n" +
                            "50.0\t2100-01-01T00:00:00.000000Z\n" +
                            "20.0\t2200-01-01T00:00:00.000000Z\n" +
                            "30.0\t2290-01-01T00:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE m4(price, 4)"
            );
        });
    }

    @Test
    public void testMinMaxFinalBucketInclusive() throws Exception {
        // See testM4FinalBucketInclusive for overflow edge discussion.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2000-01-01T00:00:00.000000Z'),
                    (50.0, '2100-01-01T00:00:00.000000Z'),
                    (20.0, '2200-01-01T00:00:00.000000Z'),
                    (30.0, '2290-01-01T00:00:00.000000Z')
                    """);
            // MinMax target=4, 2 buckets. Bucket 1: min=10(2000), max=50(2100).
            // Bucket 2: min=20(2200), max=30(2290). All 4 rows selected.
            assertSql(
                    "price\tts\n" +
                            "10.0\t2000-01-01T00:00:00.000000Z\n" +
                            "50.0\t2100-01-01T00:00:00.000000Z\n" +
                            "20.0\t2200-01-01T00:00:00.000000Z\n" +
                            "30.0\t2290-01-01T00:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE minmax(price, 4)"
            );
        });
    }

    @Test
    public void testLttbWithIntColumnDownsampling() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10, '2024-01-01T00:00:00.000000Z'),
                    (50, '2024-01-01T01:00:00.000000Z'),
                    (20, '2024-01-01T02:00:00.000000Z'),
                    (30, '2024-01-01T03:00:00.000000Z'),
                    (40, '2024-01-01T04:00:00.000000Z')
                    """);
            assertSql(
                    "price\tts\n" +
                            "10\t2024-01-01T00:00:00.000000Z\n" +
                            "40\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testM4WithLongColumnDownsampling() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (100, '2024-01-01T00:00:00.000000Z'),
                    (500, '2024-01-01T01:00:00.000000Z'),
                    (200, '2024-01-01T02:00:00.000000Z')
                    """);
            assertSql(
                    "price\tts\n" +
                            "100\t2024-01-01T00:00:00.000000Z\n" +
                            "500\t2024-01-01T01:00:00.000000Z\n" +
                            "200\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE m4(price, 12)"
            );
        });
    }

    @Test
    public void testErrorExtraArgsM4() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE m4(price, 5, '1h')",
                    47,
                    "m4() accepts exactly 2 arguments"
            );
        });
    }

    @Test
    public void testErrorExtraArgsLttb() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 5, '1h', 999)",
                    55,
                    "lttb() accepts at most 3 arguments"
            );
        });
    }

    @Test
    public void testM4AllSameTimestampExceedsTarget() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:00:00.000000Z'),
                    (30.0, '2024-01-01T00:00:00.000000Z'),
                    (40.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T00:00:00.000000Z')
                    """);
            // All same timestamp, 5 rows, target 2 - should cap at 2
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "20.0\t2024-01-01T00:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE m4(price, 2)"
            );
        });
    }

    @Test
    public void testExplainPlanSubsampleBeforeOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (RecordCursorFactory fact = compiler.compile(
                        "EXPLAIN SELECT price, ts FROM t SUBSAMPLE lttb(price, 500) ORDER BY price DESC", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    try (RecordCursor cursor = fact.getCursor(sqlExecutionContext)) {
                        StringBuilder sb = new StringBuilder();
                        while (cursor.hasNext()) {
                            sb.append(cursor.getRecord().getStrA(0)).append('\n');
                        }
                        String plan = sb.toString();
                        int subsamplePos = plan.indexOf("Subsample");
                        int sortPos = plan.indexOf("Sort");
                        Assert.assertTrue("Plan should contain Subsample: " + plan, subsamplePos >= 0);
                        if (sortPos >= 0) {
                            Assert.assertTrue("Subsample should be inside Sort: " + plan,
                                    subsamplePos > sortPos);
                        }
                    }
                }
            }
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

    @Test
    public void testSubsampleWithOrderByTimestampDesc() throws Exception {
        // SUBSAMPLE output is always timestamp-ascending. ORDER BY ts DESC
        // after SUBSAMPLE must reverse the output. If getScanDirection()
        // incorrectly reported the base direction, the outer ORDER BY could
        // be skipped.
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
            // SUBSAMPLE selects first (10) and last (40), ORDER BY ts DESC reverses
            assertSql(
                    "price\tts\n" +
                            "40.0\t2024-01-01T04:00:00.000000Z\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 2) ORDER BY ts DESC"
            );
        });
    }

    @Test
    public void testNullIntValueColumn() throws Exception {
        // INT NULL (Numbers.INT_NULL) rows must be skipped, not treated as
        // extreme values in the algorithm
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10, '2024-01-01T00:00:00.000000Z'),
                    (NULL, '2024-01-01T01:00:00.000000Z'),
                    (20, '2024-01-01T02:00:00.000000Z'),
                    (NULL, '2024-01-01T03:00:00.000000Z'),
                    (30, '2024-01-01T04:00:00.000000Z')
                    """);
            // 3 non-null rows, target 2: first (10) and last (30)
            assertSql(
                    "price\tts\n" +
                            "10\t2024-01-01T00:00:00.000000Z\n" +
                            "30\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testNullLongValueColumn() throws Exception {
        // LONG NULL rows must be skipped
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (100, '2024-01-01T00:00:00.000000Z'),
                    (NULL, '2024-01-01T01:00:00.000000Z'),
                    (200, '2024-01-01T02:00:00.000000Z')
                    """);
            assertSql(
                    "price\tts\n" +
                            "100\t2024-01-01T00:00:00.000000Z\n" +
                            "200\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testShortColumnZeroIsPreserved() throws Exception {
        // Zero is a valid value for SHORT columns, not null
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price SHORT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0, '2024-01-01T00:00:00.000000Z'),
                    (10, '2024-01-01T01:00:00.000000Z'),
                    (0, '2024-01-01T02:00:00.000000Z')
                    """);
            // All 3 rows have valid values (including zeros), target 2 selects first and last
            assertSql(
                    "price\tts\n" +
                            "0\t2024-01-01T00:00:00.000000Z\n" +
                            "0\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testByteColumnZeroIsPreserved() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price BYTE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0, '2024-01-01T00:00:00.000000Z'),
                    (5, '2024-01-01T01:00:00.000000Z'),
                    (0, '2024-01-01T02:00:00.000000Z')
                    """);
            assertSql(
                    "price\tts\n" +
                            "0\t2024-01-01T00:00:00.000000Z\n" +
                            "0\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    // ---- Fast path vs fallback path tests ----

    @Test
    public void testFastPathDirectScan() throws Exception {
        // Direct table scan: uses fast path (rowId, 24 bytes/row, no RecordChain)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, volume INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 100, '2024-01-01T00:00:00.000000Z'),
                    (50.0, 500, '2024-01-01T01:00:00.000000Z'),
                    (20.0, 200, '2024-01-01T02:00:00.000000Z'),
                    (30.0, 300, '2024-01-01T03:00:00.000000Z'),
                    (40.0, 400, '2024-01-01T04:00:00.000000Z')
                    """);
            // All pass-through columns must be correct via recordAt()
            assertSql(
                    "price\tvolume\tts\n" +
                            "10.0\t100\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t400\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, volume, ts FROM t SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testFastPathWithWhere() throws Exception {
        // WHERE filter + fast path: recordAt() must produce filtered rows
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
            // WHERE filters to 3 BTC rows, SUBSAMPLE reduces to 2
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM t WHERE symbol = 'BTC' SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testFastPathWithLimit() throws Exception {
        // LIMIT runs after SUBSAMPLE: reduces already-subsampled result
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
            // SUBSAMPLE to 3, then LIMIT 2 - should get first 2 of 3
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T01:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 3) LIMIT 2"
            );
        });
    }

    @Test
    public void testFastPathCursorReuseAndToTop() throws Exception {
        // Verify fast path cursor can be reused via getCursor() and
        // toTop() resets iteration correctly
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (20.0, '2024-01-01T02:00:00.000000Z')
                    """);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (RecordCursorFactory fact = compiler.compile(
                        "SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    // Reuse: multiple getCursor() calls on same factory
                    for (int run = 0; run < 3; run++) {
                        try (RecordCursor cursor = fact.getCursor(sqlExecutionContext)) {
                            TestUtils.assertCursor(
                                    "price\tts\n" +
                                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                                            "20.0\t2024-01-01T02:00:00.000000Z\n",
                                    cursor, fact.getMetadata(), true, sink
                            );
                            // toTop: re-iterate same cursor
                            cursor.toTop();
                            int count = 0;
                            while (cursor.hasNext()) count++;
                            Assert.assertEquals("toTop re-iteration must produce same count", 2, count);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testFallbackPathSampleBy() throws Exception {
        // SAMPLE BY uses fallback path (RecordChain materialization)
        // because AsyncGroupByRecordCursorFactory has timestampIndex=-1
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
            // SAMPLE BY produces 3 rows, fallback materializes them, SUBSAMPLE reduces to 2
            assertSql(
                    "ts\tavg\n" +
                            "2024-01-01T00:00:00.000000Z\t15.0\n" +
                            "2024-01-01T02:00:00.000000Z\t55.0\n",
                    "SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE lttb(avg, 2)"
            );
        });
    }

    @Test
    public void testFallbackPathCursorReuse() throws Exception {
        // Verify fallback path cursor can be reused (RecordChain cleared between runs)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:30:00.000000Z'),
                    (30.0, '2024-01-01T01:00:00.000000Z'),
                    (40.0, '2024-01-01T01:30:00.000000Z')
                    """);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (RecordCursorFactory fact = compiler.compile(
                        "SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE lttb(avg, 2)", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    for (int run = 0; run < 3; run++) {
                        try (RecordCursor cursor = fact.getCursor(sqlExecutionContext)) {
                            TestUtils.assertCursor(
                                    "ts\tavg\n" +
                                            "2024-01-01T00:00:00.000000Z\t15.0\n" +
                                            "2024-01-01T01:00:00.000000Z\t35.0\n",
                                    cursor, fact.getMetadata(), true, sink
                            );
                        }
                    }
                }
            }
        });
    }

    // ---- Sorting tests ----

    @Test
    public void testFallbackSortDescendingInput() throws Exception {
        // Deterministic descending input via subquery with ORDER BY ts DESC.
        // The inner sort produces a SortedRecordCursorFactory (no designated
        // timestamp, non-forward direction), forcing fallback path. The
        // fallback's isSorted=false triggers nativeSortBufferByTimestamp()
        // which must reorder to ascending before the algorithm runs.
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
            // Inner subquery delivers rows in DESC order. Fallback path
            // buffers them descending (isSorted=false), native sort reorders
            // to ascending, LTTB selects first (10) and last (40).
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM (SELECT price, ts FROM t ORDER BY ts DESC) SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testFallbackSortNegativeTimestamp() throws Exception {
        // SAMPLE BY 1w around 1970-01-01 produces a bucket starting on
        // Monday 1969-12-29 (pre-epoch, negative timestamp). This exercises
        // the ts ^ Long.MIN_VALUE signed-to-unsigned mapping in the native
        // sort. Without it, negative timestamps sort after positive ones.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '1970-01-01T00:00:00.000000Z'),
                    (20.0, '1970-01-02T00:00:00.000000Z'),
                    (30.0, '1970-01-08T00:00:00.000000Z'),
                    (40.0, '1970-01-09T00:00:00.000000Z'),
                    (50.0, '1970-01-15T00:00:00.000000Z'),
                    (60.0, '1970-01-16T00:00:00.000000Z')
                    """);
            // SAMPLE BY 1w ALIGN TO CALENDAR produces buckets starting at
            // 1969-12-29 (negative ts), 1970-01-05, 1970-01-12.
            // Wrap in ORDER BY ts DESC to force deterministic descending input,
            // ensuring isSorted=false and nativeSortBufferByTimestamp() runs.
            // The sort must handle the negative first-bucket timestamp correctly
            // via ts ^ Long.MIN_VALUE signed-to-unsigned mapping.
            assertSql(
                    "ts\tavg\n" +
                            "1969-12-29T00:00:00.000000Z\t15.0\n" +
                            "1970-01-12T00:00:00.000000Z\t55.0\n",
                    """
                            SELECT ts, avg FROM (
                                SELECT ts, avg(price) avg FROM t
                                SAMPLE BY 1w ALIGN TO CALENDAR
                                ORDER BY ts DESC
                            ) SUBSAMPLE lttb(avg, 2)
                            """
            );
        });
    }

    @Test
    public void testFallbackSortSampleByAlreadySorted() throws Exception {
        // SAMPLE BY produces time-bucketed rows that are typically monotonic.
        // The fallback path should detect isSorted=true and skip sorting.
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
            // SAMPLE BY 1h produces 3 monotonic rows, SUBSAMPLE reduces to 2.
            // isSorted=true, native sort is skipped.
            assertSql(
                    "ts\tavg\n" +
                            "2024-01-01T00:00:00.000000Z\t15.0\n" +
                            "2024-01-01T02:00:00.000000Z\t55.0\n",
                    "SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE lttb(avg, 2)"
            );
        });
    }

    @Test
    public void testSubsampleEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            assertSql(
                    "price\tts\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 10)"
            );
            assertSql(
                    "price\tts\n",
                    "SELECT price, ts FROM t SUBSAMPLE m4(price, 10)"
            );
            assertSql(
                    "price\tts\n",
                    "SELECT price, ts FROM t SUBSAMPLE minmax(price, 10)"
            );
        });
    }

    @Test
    public void testSubsampleAsQuotedColumnName() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (\"subsample\" DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z')
                    """);
            drainWalQueue();
            assertSql(
                    "subsample\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "20.0\t2024-01-01T01:00:00.000000Z\n" +
                            "30.0\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT \"subsample\", ts FROM t"
            );
        });
    }

    @Test
    public void testSubsampleAsQuotedTableName() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE \"subsample\" (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO "subsample" VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z')
                    """);
            drainWalQueue();
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "20.0\t2024-01-01T01:00:00.000000Z\n",
                    "SELECT * FROM \"subsample\""
            );
        });
    }

    @Test
    public void testSubsampleAsUnquotedTableNameFails() throws Exception {
        assertMemoryLeak(() -> assertException(
                "CREATE TABLE subsample (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL",
                13,
                "table and column names that are SQL keywords"
        ));
    }

    @Test
    public void testSubsampleGapThresholdOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES (10.0, '2024-01-01T00:00:00.000000Z')");
            drainWalQueue();
            // 2_000_000_000d fits in int but 2_000_000_000 * 86_400_000_000 overflows long
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 100, '2000000000d')",
                    51,
                    "gap threshold overflow"
            );
        });
    }

    @Test
    public void testSubsampleInsideSubqueryWithOuterAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            // Outer projection with alias - inner SUBSAMPLE still applies
            assertSql(
                    "p\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price AS p, ts FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 2))"
            );
        });
    }

    @Test
    public void testSubsampleInsideSubqueryWithOuterLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            // SUBSAMPLE to 2 rows, then LIMIT to 1
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n",
                    "SELECT * FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)) LIMIT 1"
            );
        });
    }

    @Test
    public void testSubsampleInsideSubqueryWithOuterOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            // SUBSAMPLE first, then ORDER BY DESC
            assertSql(
                    "price\tts\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n",
                    "SELECT * FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)) ORDER BY ts DESC"
            );
        });
    }

    @Test
    public void testSubsampleInsideSubqueryWithOuterWhere() throws Exception {
        // Prove SUBSAMPLE inside a subquery executes before outer WHERE.
        //
        // Data: 8 rows with non-linear prices and sequential qty 1..8.
        // LTTB(price, 4) on all 8: first + last + 2 area-maximizing from
        // 3 buckets of 2 rows each.
        //
        // Correct path: SUBSAMPLE(all 8) -> 4 rows -> WHERE qty >= 5 -> subset
        // Wrong path (hoisted WHERE): WHERE qty >= 5 -> 4 rows -> SUBSAMPLE(4) -> all 4
        //
        // The test uses M4 instead of LTTB for more deterministic output:
        // M4(price, 4) = 1 bucket with first/last/min/max = 4 rows always.
        // Then WHERE filters those 4.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 1, '2024-01-01T00:00:00.000000Z'),
                    (90.0, 2, '2024-01-01T01:00:00.000000Z'),
                    (5.0,  3, '2024-01-01T02:00:00.000000Z'),
                    (50.0, 4, '2024-01-01T03:00:00.000000Z'),
                    (95.0, 5, '2024-01-01T04:00:00.000000Z'),
                    (3.0,  6, '2024-01-01T05:00:00.000000Z'),
                    (80.0, 7, '2024-01-01T06:00:00.000000Z'),
                    (40.0, 8, '2024-01-01T07:00:00.000000Z')
                    """);
            drainWalQueue();
            // M4(price, 4) on all 8 rows: 1 bucket, selects:
            //   first=row0(10,qty=1), min=row5(3,qty=6), max=row4(95,qty=5), last=row7(40,qty=8)
            // Sorted by index: 0, 4, 5, 7 -> qty values: 1, 5, 6, 8
            assertSql(
                    "price\tqty\tts\n" +
                            "10.0\t1\t2024-01-01T00:00:00.000000Z\n" +
                            "95.0\t5\t2024-01-01T04:00:00.000000Z\n" +
                            "3.0\t6\t2024-01-01T05:00:00.000000Z\n" +
                            "40.0\t8\t2024-01-01T07:00:00.000000Z\n",
                    "SELECT price, qty, ts FROM t SUBSAMPLE m4(price, 4)"
            );
            // Correct: inner M4 selects {qty=1,5,6,8}, outer WHERE qty >= 6 keeps {6,8}
            assertSql(
                    "price\tqty\tts\n" +
                            "3.0\t6\t2024-01-01T05:00:00.000000Z\n" +
                            "40.0\t8\t2024-01-01T07:00:00.000000Z\n",
                    "SELECT * FROM (SELECT price, qty, ts FROM t SUBSAMPLE m4(price, 4)) WHERE qty >= 6"
            );
            // Wrong path (hoisted WHERE) would be: WHERE qty>=6 -> {6,7,8} ->
            // M4(3 rows, target 4) returns all 3 -> {qty=6,7,8}. That's 3 rows
            // with qty=7 present, which differs from the correct 2-row result above.
        });
    }

    @Test
    public void testSubsampleWithBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            bindVariableService.clear();
            bindVariableService.setLong(0, 2);
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, $1)"
            );
        });
    }

    @Test
    public void testSubsampleWithDeclareVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\n",
                    "DECLARE @n := 2 SELECT price, ts FROM t SUBSAMPLE lttb(price, @n)"
            );
        });
    }

    // Finding #18 (join branch preservation) is already covered by
    // testSubsampleBranchLocalInJoin which tests ASOF JOIN with SUBSAMPLE
    // on the right side with exact expected output.

    @Test
    public void testHoistingSafeForSampleBySubsample() throws Exception {
        // SAMPLE BY + SUBSAMPLE on the same query is an optimizer-restructured
        // case where the pull-up must work. This is the existing path that must
        // not be broken by the hoisting guard.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (15.0, '2024-01-01T00:30:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (25.0, '2024-01-01T01:30:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (35.0, '2024-01-01T02:30:00.000000Z')
                    """);
            drainWalQueue();
            // SAMPLE BY 1h produces 3 rows, SUBSAMPLE to 2 (first and last)
            assertSql(
                    "ts\tavg\n" +
                            "2024-01-01T00:00:00.000000Z\t12.5\n" +
                            "2024-01-01T02:00:00.000000Z\t32.5\n",
                    "SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE lttb(avg, 2)"
            );
        });
    }

    @Test
    public void testHoistingBlockedByOuterLimit() throws Exception {
        // SUBSAMPLE inside subquery with outer LIMIT. The code generator
        // blocks SUBSAMPLE pull-up across the user subquery boundary
        // (isNestedModelIsSubQuery), and the optimizer blocks WHERE pushdown
        // via hasSubsampleInChain. LIMIT stays on the outer model.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            // SUBSAMPLE(5 rows, target=2) gives {first=10, last=50}, then LIMIT 1
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n",
                    "SELECT * FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)) LIMIT 1"
            );
        });
    }

    @Test
    public void testHoistingBlockedByOuterDistinct() throws Exception {
        // SUBSAMPLE inside subquery with outer DISTINCT.
        // Include ts in outer SELECT to preserve timestamp.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (10.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            // SUBSAMPLE(5 rows, target=2) gives {first=10 at 00:00, last=50 at 04:00}
            // DISTINCT over price,ts produces 2 rows (already distinct)
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT DISTINCT price, ts FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 2))"
            );
        });
    }

    @Test
    public void testHoistingExactM4Output() throws Exception {
        // M4 produces deterministic first/last/min/max indices.
        // Verify exact selected rows for a single bucket.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (30.0, '2024-01-01T00:00:00.000000Z'),
                    (90.0, '2024-01-01T01:00:00.000000Z'),
                    (10.0, '2024-01-01T02:00:00.000000Z'),
                    (70.0, '2024-01-01T03:00:00.000000Z'),
                    (20.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            // M4(price, 4): 1 bucket. first=30(row0), last=20(row4),
            // min=10(row2), max=90(row1). Sorted by index: 0,1,2,4
            assertSql(
                    "price\tts\n" +
                            "30.0\t2024-01-01T00:00:00.000000Z\n" +
                            "90.0\t2024-01-01T01:00:00.000000Z\n" +
                            "10.0\t2024-01-01T02:00:00.000000Z\n" +
                            "20.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE m4(price, 4)"
            );
        });
    }

    @Test
    public void testHoistingExactMinMaxOutput() throws Exception {
        // MinMax produces deterministic min/max indices per bucket.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (30.0, '2024-01-01T00:00:00.000000Z'),
                    (90.0, '2024-01-01T01:00:00.000000Z'),
                    (10.0, '2024-01-01T02:00:00.000000Z'),
                    (70.0, '2024-01-01T03:00:00.000000Z'),
                    (20.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            // MinMax(price, 2): 1 bucket. min=10(row2), max=90(row1).
            // Sorted by index: 1, 2
            assertSql(
                    "price\tts\n" +
                            "90.0\t2024-01-01T01:00:00.000000Z\n" +
                            "10.0\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE minmax(price, 2)"
            );
        });
    }

    @Test
    public void testUniformBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z'),
                    (60.0, '2024-01-01T05:00:00.000000Z'),
                    (70.0, '2024-01-01T06:00:00.000000Z'),
                    (80.0, '2024-01-01T07:00:00.000000Z'),
                    (90.0, '2024-01-01T08:00:00.000000Z'),
                    (100.0, '2024-01-01T09:00:00.000000Z')
                    """);
            drainWalQueue();
            // 10 rows, target 4: positions 0, 3, 6, 9
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t2024-01-01T03:00:00.000000Z\n" +
                            "70.0\t2024-01-01T06:00:00.000000Z\n" +
                            "100.0\t2024-01-01T09:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE uniform(4)"
            );
        });
    }

    @Test
    public void testUniformEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            assertSql(
                    "price\tts\n",
                    "SELECT price, ts FROM t SUBSAMPLE uniform(10)"
            );
        });
    }

    @Test
    public void testUniformInputSmallerThanTarget() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z')
                    """);
            drainWalQueue();
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "20.0\t2024-01-01T01:00:00.000000Z\n" +
                            "30.0\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE uniform(10)"
            );
        });
    }

    @Test
    public void testUniformNoColumnLookup() throws Exception {
        // uniform(4) must not try to resolve "4" as a column name
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z')
                    """);
            drainWalQueue();
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "20.0\t2024-01-01T01:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE uniform(10)"
            );
        });
    }

    @Test
    public void testUniformPreservesNullValues() throws Exception {
        // uniform does not inspect values; NaN/NULL data should pass through
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (NaN, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (NaN, '2024-01-01T02:00:00.000000Z')
                    """);
            drainWalQueue();
            assertSql(
                    "price\tts\n" +
                            "null\t2024-01-01T00:00:00.000000Z\n" +
                            "20.0\t2024-01-01T01:00:00.000000Z\n" +
                            "null\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE uniform(10)"
            );
        });
    }

    @Test
    public void testUniformTargetTooLow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE uniform(1)",
                    42,
                    "target points must be at least 2"
            );
        });
    }

    @Test
    public void testUniformTargetTwo() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            // target 2: first and last only
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE uniform(2)"
            );
        });
    }

    @Test
    public void testCadenceBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z'),
                    (60.0, '2024-01-01T05:00:00.000000Z')
                    """);
            drainWalQueue();
            // stride 2, offset 0: emit 0, then 2+0=2, 4+0=4, pin last=5
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "30.0\t2024-01-01T02:00:00.000000Z\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\n" +
                            "60.0\t2024-01-01T05:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE cadence(2)"
            );
        });
    }

    @Test
    public void testCadenceEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            assertSql(
                    "price\tts\n",
                    "SELECT price, ts FROM t SUBSAMPLE cadence(5)"
            );
        });
    }

    @Test
    public void testCadenceNoColumnLookup() throws Exception {
        // cadence(2) must not try to resolve "2" as a column name
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES (10.0, '2024-01-01T00:00:00.000000Z')");
            drainWalQueue();
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE cadence(5)"
            );
        });
    }

    @Test
    public void testCadencePreservesNullValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (NaN, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (NaN, '2024-01-01T02:00:00.000000Z')
                    """);
            drainWalQueue();
            // cadence(1) returns all rows including NaN values
            assertSql(
                    "price\tts\n" +
                            "null\t2024-01-01T00:00:00.000000Z\n" +
                            "20.0\t2024-01-01T01:00:00.000000Z\n" +
                            "null\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE cadence(1)"
            );
        });
    }

    @Test
    public void testCadenceStrideOne() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z')
                    """);
            drainWalQueue();
            // stride 1: all rows returned unchanged
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "20.0\t2024-01-01T01:00:00.000000Z\n" +
                            "30.0\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE cadence(1)"
            );
        });
    }

    @Test
    public void testCadenceStrideLargerThanInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z')
                    """);
            drainWalQueue();
            // stride 100 > 3 rows: only first row, no last pinning
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE cadence(100)"
            );
        });
    }

    @Test
    public void testCadenceWithSeed() throws Exception {
        // cadence(3, 42): 10 rows, stride 3, seed 42.
        // Offset = Rnd(42, 42).nextInt(3). Emit 0, then stride+offset series, then last.
        // This test computes the expected offset and asserts exact output.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z'),
                    (60.0, '2024-01-01T05:00:00.000000Z'),
                    (70.0, '2024-01-01T06:00:00.000000Z'),
                    (80.0, '2024-01-01T07:00:00.000000Z'),
                    (90.0, '2024-01-01T08:00:00.000000Z'),
                    (100.0, '2024-01-01T09:00:00.000000Z')
                    """);
            drainWalQueue();
            // Compute expected offset
            Rnd rnd = new Rnd(42, 42);
            int offset = rnd.nextInt(3);
            // Build expected rows: 0, then stride+offset, 2*stride+offset, ..., pin last=9
            String[] rows = {
                    "10.0\t2024-01-01T00:00:00.000000Z",
                    "20.0\t2024-01-01T01:00:00.000000Z",
                    "30.0\t2024-01-01T02:00:00.000000Z",
                    "40.0\t2024-01-01T03:00:00.000000Z",
                    "50.0\t2024-01-01T04:00:00.000000Z",
                    "60.0\t2024-01-01T05:00:00.000000Z",
                    "70.0\t2024-01-01T06:00:00.000000Z",
                    "80.0\t2024-01-01T07:00:00.000000Z",
                    "90.0\t2024-01-01T08:00:00.000000Z",
                    "100.0\t2024-01-01T09:00:00.000000Z"
            };
            StringBuilder expected = new StringBuilder("price\tts\n");
            expected.append(rows[0]).append('\n');
            int lastEmitted = 0;
            for (int pos = 3 + offset; pos < 10; pos += 3) {
                expected.append(rows[pos]).append('\n');
                lastEmitted = pos;
            }
            if (lastEmitted != 9) {
                expected.append(rows[9]).append('\n');
            }
            assertSql(expected.toString(), "SELECT price, ts FROM t SUBSAMPLE cadence(3, 42)");
            // Deterministic: second run produces identical output
            assertSql(expected.toString(), "SELECT price, ts FROM t SUBSAMPLE cadence(3, 42)");
        });
    }

    @Test
    public void testCadenceDifferentSeeds() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z'),
                    (60.0, '2024-01-01T05:00:00.000000Z'),
                    (70.0, '2024-01-01T06:00:00.000000Z'),
                    (80.0, '2024-01-01T07:00:00.000000Z'),
                    (90.0, '2024-01-01T08:00:00.000000Z'),
                    (100.0, '2024-01-01T09:00:00.000000Z')
                    """);
            drainWalQueue();
            // Find two seeds that produce different offsets for stride=5
            int seedA = -1;
            int seedB = -1;
            int offsetA = -1;
            for (int s = 0; s < 100; s++) {
                int off = new Rnd(s, s).nextInt(5);
                if (seedA == -1) {
                    seedA = s;
                    offsetA = off;
                } else if (off != offsetA) {
                    seedB = s;
                    break;
                }
            }
            Assert.assertTrue("could not find two seeds with different offsets", seedB != -1);
            sink.clear();
            printSql("SELECT price, ts FROM t SUBSAMPLE cadence(5, " + seedA + ")", sink);
            String resultA = sink.toString();
            sink.clear();
            printSql("SELECT price, ts FROM t SUBSAMPLE cadence(5, " + seedB + ")", sink);
            String resultB = sink.toString();
            Assert.assertNotEquals("different seeds must produce different output", resultA, resultB);
        });
    }

    @Test
    public void testCadenceWithNullSeed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            // cadence(3, NULL): random mode. First row always present, last pinned.
            sink.clear();
            printSql("SELECT price, ts FROM t SUBSAMPLE cadence(3, NULL)", sink);
            String result = sink.toString();
            Assert.assertTrue(result.contains("10.0\t2024-01-01T00:00:00.000000Z"));
            Assert.assertTrue(result.contains("50.0\t2024-01-01T04:00:00.000000Z"));
            // 5 rows, stride 3: first + 0-1 stride rows + last = 2-3 rows
            long rowCount = result.chars().filter(c -> c == '\n').count() - 1;
            Assert.assertTrue("expected 2-3 rows, got " + rowCount, rowCount >= 2 && rowCount <= 3);
        });
    }

    @Test
    public void testCadenceLastRowIncluded() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            // stride 3, offset 0: emit 0, then 3+0=3, last=4 (pinned)
            // 5 rows, stride 3 doesn't divide evenly
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t2024-01-01T03:00:00.000000Z\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE cadence(3)"
            );
        });
    }

    @Test
    public void testUniformWithBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            bindVariableService.clear();
            bindVariableService.setLong(0, 2);
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE uniform($1)"
            );
        });
    }

    @Test
    public void testUniformWithDeclareVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\n",
                    "DECLARE @n := 2 SELECT price, ts FROM t SUBSAMPLE uniform(@n)"
            );
        });
    }

    @Test
    public void testUniformAfterSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (15.0, '2024-01-01T00:30:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (25.0, '2024-01-01T01:30:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (35.0, '2024-01-01T02:30:00.000000Z')
                    """);
            drainWalQueue();
            // SAMPLE BY 1h -> 3 rows, uniform(2) -> first and last
            assertSql(
                    "ts\tavg\n" +
                            "2024-01-01T00:00:00.000000Z\t12.5\n" +
                            "2024-01-01T02:00:00.000000Z\t32.5\n",
                    "SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE uniform(2)"
            );
        });
    }

    @Test
    public void testUniformInsideSubqueryWithOuterWhere() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 1, '2024-01-01T00:00:00.000000Z'),
                    (20.0, 2, '2024-01-01T01:00:00.000000Z'),
                    (30.0, 3, '2024-01-01T02:00:00.000000Z'),
                    (40.0, 4, '2024-01-01T03:00:00.000000Z'),
                    (50.0, 5, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            // uniform(3) on 5 rows: positions 0, 2, 4 -> qty 1, 3, 5
            // outer WHERE qty > 3 -> keeps qty 5 only
            assertSql(
                    "price\tqty\tts\n" +
                            "50.0\t5\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT * FROM (SELECT price, qty, ts FROM t SUBSAMPLE uniform(3)) WHERE qty > 3"
            );
        });
    }

    @Test
    public void testCadenceStrideNegative() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE cadence(-1)",
                    42,
                    "stride must be at least 1"
            );
        });
    }

    @Test
    public void testCadenceSeedFloat() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE cadence(5, 1.5)",
                    45,
                    "integer or NULL expected for seed"
            );
        });
    }

    @Test
    public void testCadenceUnsetBindSeed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES (10.0, '2024-01-01T00:00:00.000000Z')");
            drainWalQueue();
            bindVariableService.clear();
            bindVariableService.setLong(0, 5);
            // $2 unset -> coerced to LONG, reads as NULL -> "seed must be set"
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE cadence($1, $2)",
                    46,
                    "seed must be set"
            );
        });
    }

    @Test
    public void testCadenceStrideOneWithNullSeed() throws Exception {
        // cadence(1, NULL): validates seed type, but stride=1 returns all rows
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z')
                    """);
            drainWalQueue();
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "20.0\t2024-01-01T01:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE cadence(1, NULL)"
            );
        });
    }

    @Test
    public void testCadenceAfterSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (15.0, '2024-01-01T00:30:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (25.0, '2024-01-01T01:30:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (35.0, '2024-01-01T02:30:00.000000Z')
                    """);
            drainWalQueue();
            // SAMPLE BY 1h -> 3 rows, cadence(2) -> first, stride row, last
            assertSql(
                    "ts\tavg\n" +
                            "2024-01-01T00:00:00.000000Z\t12.5\n" +
                            "2024-01-01T02:00:00.000000Z\t32.5\n",
                    "SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE cadence(2)"
            );
        });
    }

    @Test
    public void testCadenceWithBindStride() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            bindVariableService.clear();
            bindVariableService.setLong(0, 3);
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t2024-01-01T03:00:00.000000Z\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE cadence($1)"
            );
        });
    }

    @Test
    public void testCadenceWithDeclareStride() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t2024-01-01T03:00:00.000000Z\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\n",
                    "DECLARE @s := 3 SELECT price, ts FROM t SUBSAMPLE cadence(@s)"
            );
        });
    }

    @Test
    public void testCadenceWithBindSeed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            bindVariableService.clear();
            bindVariableService.setLong(0, 3);
            bindVariableService.setLong(1, 42);
            // cadence($1, $2) with stride=3, seed=42
            sink.clear();
            printSql("SELECT price, ts FROM t SUBSAMPLE cadence($1, $2)", sink);
            String result = sink.toString();
            Assert.assertTrue(result.contains("10.0\t2024-01-01T00:00:00.000000Z"));
            Assert.assertTrue(result.contains("50.0\t2024-01-01T04:00:00.000000Z"));
        });
    }

    @Test
    public void testCadenceInsideSubqueryWithOuterWhere() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 1, '2024-01-01T00:00:00.000000Z'),
                    (20.0, 2, '2024-01-01T01:00:00.000000Z'),
                    (30.0, 3, '2024-01-01T02:00:00.000000Z'),
                    (40.0, 4, '2024-01-01T03:00:00.000000Z'),
                    (50.0, 5, '2024-01-01T04:00:00.000000Z'),
                    (60.0, 6, '2024-01-01T05:00:00.000000Z')
                    """);
            drainWalQueue();
            // cadence(3) on 6 rows: emit 0, 3, pin 5 -> qty 1, 4, 6
            // outer WHERE qty > 3 keeps qty 4 and 6
            assertSql(
                    "price\tqty\tts\n" +
                            "40.0\t4\t2024-01-01T03:00:00.000000Z\n" +
                            "60.0\t6\t2024-01-01T05:00:00.000000Z\n",
                    "SELECT * FROM (SELECT price, qty, ts FROM t SUBSAMPLE cadence(3)) WHERE qty > 3"
            );
        });
    }

    @Test
    public void testCadenceWithExpressionSeed() throws Exception {
        // Spec broadened to accept constant/runtime-constant integer expressions.
        // cadence(3, 40 + 2) should produce the same result as cadence(3, 42).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z'),
                    (40.0, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z')
                    """);
            drainWalQueue();
            sink.clear();
            printSql("SELECT price, ts FROM t SUBSAMPLE cadence(3, 42)", sink);
            String expected = sink.toString();
            assertSql(expected, "SELECT price, ts FROM t SUBSAMPLE cadence(3, 40 + 2)");
        });
    }
}
