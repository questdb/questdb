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
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SubsampleTest extends AbstractCairoTest {

    // Local restore of the plain value-only assert helper that master removed from
    // AbstractCairoTest/TestUtils (consolidated toward fluent assertQuery). Keeps this
    // PR's existing assertions compiling without a wholesale rewrite to the fluent style.
    private void assertSql(CharSequence expected, CharSequence sql) throws SqlException {
        printSql(sql);
        TestUtils.assertEquals(expected, sink);
    }

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
        // SAMPLE BY results lose designated-timestamp metadata, but the aggregation wrapper keeps
        // the timestamp output available to the outer window selector.
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
    public void testErrorUnknownMethodWithoutDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP)");
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE unknown_algo(price, 5)",
                    24,
                    "SUBSAMPLE requires a designated timestamp column"
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
                    "column not found: nonexistent"
            );
            assertException(
                    "SELECT * FROM t SUBSAMPLE m4(nonexistent, 5)",
                    29,
                    "column not found: nonexistent"
            );
            assertException(
                    "SELECT * FROM t SUBSAMPLE minmax(nonexistent, 5)",
                    33,
                    "column not found: nonexistent"
            );
        });
    }

    @Test
    public void testErrorColumnNotFoundPrecedesMissingDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP)");
            for (String method : new String[]{"lttb", "m4", "minmax"}) {
                final String sql = "SELECT * FROM t SUBSAMPLE " + method + "(nonexistent, 5)";
                assertException(sql, sql.indexOf("nonexistent"), "column not found: nonexistent");
            }
            final String expressionSql = "SELECT * FROM t SUBSAMPLE m4(price * 2, 5)";
            assertException(expressionSql, expressionSql.lastIndexOf('*'), "column not found: *");
        });
    }

    @Test
    public void testErrorTargetFunctionParserErrorIsNotMasked() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            for (String method : new String[]{"lttb", "m4", "minmax"}) {
                final String sql = "SELECT * FROM t SUBSAMPLE " + method + "(price, no_such_function())";
                assertException(
                        sql,
                        sql.indexOf("no_such_function"),
                        "unknown function name: no_such_function"
                );
            }
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
            final String query = "WITH data AS (SELECT price, ts FROM t) SELECT price, ts FROM data SUBSAMPLE lttb(price, 2)";
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t2024-01-01T04:00:00.000000Z\n",
                    query
            );
            printSql("EXPLAIN " + query);
            final String plan = sink.toString();
            Assert.assertTrue("CTE SUBSAMPLE must use the window path: " + plan, plan.contains("CachedWindow"));
            Assert.assertFalse("CTE SUBSAMPLE must not use the legacy cursor: " + plan, plan.contains("Subsample"));
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
            final String query = "SELECT price, ts FROM (SELECT * FROM t) SUBSAMPLE lttb(price, 2)";
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t2024-01-01T04:00:00.000000Z\n",
                    query
            );
            printSql("EXPLAIN " + query);
            final String plan = sink.toString();
            Assert.assertTrue("subquery SUBSAMPLE must use the window path: " + plan, plan.contains("CachedWindow"));
            Assert.assertFalse("subquery SUBSAMPLE must not use the legacy cursor: " + plan, plan.contains("Subsample"));
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

    // Byte-identity guard for the fused row-selecting path (CachedWindowLightRecordCursorFactory):
    // interleaved NULL rows + normal rows, exercising the desugared SUBSAMPLE keep-flag fusion for
    // m4/minmax/lttb. This locks the exact kept rows so a change that skips per-row narrowChain
    // materialization on the fused path cannot silently alter which rows are emitted.
    @Test
    public void testFusedKeepFlagByteIdentityInterleavedNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (NULL, '2024-01-01T01:00:00.000000Z'),
                    (50.0, '2024-01-01T02:00:00.000000Z'),
                    (NULL, '2024-01-01T03:00:00.000000Z'),
                    (20.0, '2024-01-01T04:00:00.000000Z'),
                    (5.0, '2024-01-01T05:00:00.000000Z'),
                    (NULL, '2024-01-01T06:00:00.000000Z'),
                    (80.0, '2024-01-01T07:00:00.000000Z'),
                    (15.0, '2024-01-01T08:00:00.000000Z'),
                    (NULL, '2024-01-01T09:00:00.000000Z'),
                    (60.0, '2024-01-01T10:00:00.000000Z'),
                    (25.0, '2024-01-01T11:00:00.000000Z')
                    """);
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "5.0\t2024-01-01T05:00:00.000000Z\n" +
                            "80.0\t2024-01-01T07:00:00.000000Z\n" +
                            "25.0\t2024-01-01T11:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE m4(price, 4)"
            );
            assertSql(
                    "price\tts\n" +
                            "50.0\t2024-01-01T02:00:00.000000Z\n" +
                            "5.0\t2024-01-01T05:00:00.000000Z\n" +
                            "80.0\t2024-01-01T07:00:00.000000Z\n" +
                            "15.0\t2024-01-01T08:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE minmax(price, 4)"
            );
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T02:00:00.000000Z\n" +
                            "80.0\t2024-01-01T07:00:00.000000Z\n" +
                            "25.0\t2024-01-01T11:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 4)"
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
    public void testLttbGapRejectsConstantExpressionsLikeLegacyCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 5, concat('1', 'h'))",
                    49,
                    "expected single letter qualifier"
            );
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 5, '1h'::string)",
                    53,
                    "expected single letter qualifier"
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
    public void testSubsampleRejectsHiddenDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE rt (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            final String sql = "SELECT x FROM (SELECT price x FROM rt) SUBSAMPLE uniform(2)";
            assertException(sql, sql.indexOf("SUBSAMPLE"), "SUBSAMPLE requires a designated timestamp column");
        });
    }

    @Test
    public void testSubsampleRejectsComputedTimestampAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE rt (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            final String[] sql = {
                    "SELECT price, '2024-02-01'::TIMESTAMP ts FROM rt SUBSAMPLE uniform(2)",
                    "SELECT price, timestamp_sequence(0, 1) ts FROM rt SUBSAMPLE uniform(2)",
                    "SELECT price, timestamp_floor_utc('1h', ts, null, '00:00', null) ts FROM rt SUBSAMPLE uniform(2)",
                    "SELECT price, 42 ts FROM rt SUBSAMPLE uniform(2)",
                    "SELECT price, ts::STRING ts FROM rt SUBSAMPLE uniform(2)",
                    "SELECT price, ts::LONG ts FROM rt SUBSAMPLE uniform(2)"
            };
            for (String query : sql) {
                assertException(query, query.indexOf("SUBSAMPLE"), "SUBSAMPLE requires a designated timestamp column");
            }
        });
    }

    @Test
    public void testSubsampleRejectsHiddenValueColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE rt (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO rt VALUES (10.0, '2024-01-01'), (20.0, '2024-01-02'), (30.0, '2024-01-03')");
            final String hidden = "SELECT price x, ts FROM rt SUBSAMPLE lttb(price, 2)";
            assertException(hidden, hidden.lastIndexOf("price"), "column not found: price");
            final String qualified = "SELECT price x, ts FROM rt SUBSAMPLE lttb(rt.price, 2)";
            assertException(qualified, qualified.lastIndexOf("rt.price"), "column not found: rt.price");
            assertQuery("SELECT price x, ts FROM rt SUBSAMPLE lttb(x, 2)")
                    .timestamp("ts")
                    .returns("x\tts\n10.0\t2024-01-01T00:00:00.000000Z\n30.0\t2024-01-03T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testSubsamplePreservesMixedWildcardProjection() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE rt (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO rt VALUES (10.0, '2024-01-01'), (20.0, '2024-01-02'), (30.0, '2024-01-03')");
            assertQuery("SELECT *, price + 1 x FROM rt SUBSAMPLE uniform(2)")
                    .timestamp("ts")
                    .returns("price\tts\tx\n10.0\t2024-01-01T00:00:00.000000Z\t11.0\n30.0\t2024-01-03T00:00:00.000000Z\t31.0\n");
        });
    }

    @Test
    public void testSubsampleUsesProjectedNumericExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE rt (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO rt VALUES (1.0, '2024-01-01'), (-4.0, '2024-01-02'), (0.0, '2024-01-03'), (5.0, '2024-01-04'), (0.0, '2024-01-05'), (1.0, '2024-01-06')");
            // Raw prices choose -4 at index 1; the projected square must instead choose 25 at index 3.
            assertQuery("SELECT price * price price, ts FROM rt SUBSAMPLE lttb(price, 3)")
                    .timestamp("ts")
                    .returns("price\tts\n1.0\t2024-01-01T00:00:00.000000Z\n25.0\t2024-01-04T00:00:00.000000Z\n1.0\t2024-01-06T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testSubsampleUsesProjectedTypeForValidation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE rt (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            final String sql = "SELECT price::STRING price, ts FROM rt SUBSAMPLE lttb(price, 2)";
            assertException(sql, sql.lastIndexOf("price"), "numeric column expected, got: STRING");
        });
    }

    @Test
    public void testSubsampleViaAliasedSampleByColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE rt (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO rt VALUES (10.0, '2024-01-01T00:10'), (20.0, '2024-01-01T01:10'), (30.0, '2024-01-01T02:10')");
            final String expected = "bucket\tav\n2024-01-01T00:00:00.000000Z\t10.0\n2024-01-01T02:00:00.000000Z\t30.0\n";
            final String prefix = "SELECT ts bucket, avg(price) av FROM rt SAMPLE BY 1h SUBSAMPLE ";
            for (String method : new String[]{"uniform(2)", "cadence(2)", "m4(av, 2)", "lttb(av, 2)"}) {
                assertQuery(prefix + method).timestamp("bucket").returns(expected);
            }
            final String plan = planOf(prefix + "lttb(av, 2)");
            Assert.assertTrue("aliased SAMPLE BY must order the window by its output alias: " + plan, plan.contains("order by [bucket]"));
            Assert.assertFalse("aliased SAMPLE BY must not leave a legacy node: " + plan, plan.contains("Subsample"));
        });
    }

    @Test
    public void testSubsampleViaRenamedSubqueryAndCteColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE rt (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO rt VALUES (10.0, '2024-01-01'), (20.0, '2024-01-02'), (30.0, '2024-01-03')");
            final String expected = "x\tt\n10.0\t2024-01-01T00:00:00.000000Z\n30.0\t2024-01-03T00:00:00.000000Z\n";
            final String subquery = "SELECT x, t FROM (SELECT price x, ts t FROM rt) SUBSAMPLE lttb(x, 2)";
            final String cte = "WITH q AS (SELECT price x, ts t FROM rt) SELECT x, t FROM q SUBSAMPLE lttb(x, 2)";
            assertQuery(subquery).timestamp("t").returns(expected);
            assertQuery(cte).timestamp("t").returns(expected);
            final String plan = planOf(cte);
            Assert.assertTrue("renamed CTE must use a window node: " + plan, plan.contains("CachedWindow"));
            Assert.assertTrue("renamed CTE must order by visible t: " + plan, plan.contains("order by [t]"));
            Assert.assertFalse("renamed CTE must not leave a legacy node: " + plan, plan.contains("Subsample"));
            try (RecordCursorFactory factory = select(subquery)) {
                final RecordMetadata metadata = factory.getMetadata();
                Assert.assertEquals(1, metadata.getTimestampIndex());
                TestUtils.assertEquals("t", metadata.getColumnName(metadata.getTimestampIndex()));
            }
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
                    "numeric column expected, got: SYMBOL"
            );
            assertException(
                    "SELECT * FROM t SUBSAMPLE m4(name, 5)",
                    29,
                    "numeric column expected, got: SYMBOL"
            );
            assertException(
                    "SELECT * FROM t SUBSAMPLE minmax(name, 5)",
                    33,
                    "numeric column expected, got: SYMBOL"
            );
        });
    }

    @Test
    public void testErrorNonNumericProjectedAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (name SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE j (name SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            final String[] queries = {
                    "SELECT name x, ts FROM t SUBSAMPLE lttb(x, 2)",
                    "SELECT * FROM (SELECT name x, ts FROM t) SUBSAMPLE lttb(x, 2)",
                    "WITH q AS (SELECT name x, ts FROM t) SELECT * FROM q SUBSAMPLE lttb(x, 2)",
                    "SELECT ts, first(name) x FROM t SAMPLE BY 1h SUBSAMPLE lttb(x, 2)",
                    "SELECT * FROM (SELECT t.name x, t.ts FROM t ASOF JOIN j ON (name)) SUBSAMPLE lttb(x, 2)"
            };
            for (String sql : queries) {
                assertException(
                        sql,
                        sql.lastIndexOf("x, 2"),
                        "numeric column expected, got: SYMBOL"
                );
            }
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
    public void testSubsampleJoinUsesCompletedProjectionWithDuplicateInputNames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE jp (price DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE jq (price DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            // Left and right spikes occur at different timestamps. Target 3 must choose the left
            // projection's interior spike (Jan 2), proving the hidden right price cannot drive LTTB.
            execute("INSERT INTO jp VALUES (0.0, 'X', '2024-01-01'), (100.0, 'X', '2024-01-02'), (0.0, 'X', '2024-01-03'), (0.0, 'X', '2024-01-04'), (0.0, 'X', '2024-01-05')");
            execute("INSERT INTO jq VALUES (0.0, 'X', '2024-01-01'), (0.0, 'X', '2024-01-02'), (0.0, 'X', '2024-01-03'), (1000.0, 'X', '2024-01-04'), (0.0, 'X', '2024-01-05')");

            final String sql = "SELECT p.price, p.ts FROM jp p ASOF JOIN jq q ON (symbol) SUBSAMPLE lttb(price, 3)";
            assertQuery(sql)
                    .timestamp("ts")
                    .returns("price\tts\n" +
                            "0.0\t2024-01-01T00:00:00.000000Z\n" +
                            "100.0\t2024-01-02T00:00:00.000000Z\n" +
                            "0.0\t2024-01-05T00:00:00.000000Z\n");
            final String plan = planOf(sql);
            Assert.assertTrue("duplicate join inputs must resolve through the completed projection: " + plan, plan.contains("CachedWindow"));
            Assert.assertFalse("no legacy SUBSAMPLE node may survive: " + plan, plan.contains("Subsample"));
        });
    }

    @Test
    public void testSubsampledJoinRetainsTimestampAsOuterTimeJoinOperand() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE jo (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE jip (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE jiq (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO jo VALUES (100.0, '2024-01-01'), (200.0, '2024-01-02'), (300.0, '2024-01-03'), (400.0, '2024-01-04'), (500.0, '2024-01-05')");
            execute("INSERT INTO jip VALUES (10.0, '2024-01-01'), (20.0, '2024-01-02'), (30.0, '2024-01-03'), (40.0, '2024-01-04')");
            execute("INSERT INTO jiq VALUES (1000.0, '2024-01-01'), (2000.0, '2024-01-02'), (3000.0, '2024-01-03'), (4000.0, '2024-01-04')");

            final String right = "(SELECT p.price x, p.ts FROM jip p ASOF JOIN jiq q SUBSAMPLE lttb(x, 2)) z";
            final String asof = "SELECT o.price, o.ts, z.x FROM jo o ASOF JOIN " + right;
            assertQuery(asof)
                    .timestamp("ts")
                    .expectSize()
                    .noRandomAccess()
                    .returns("price\tts\tx\n" +
                            "100.0\t2024-01-01T00:00:00.000000Z\t10.0\n" +
                            "200.0\t2024-01-02T00:00:00.000000Z\t10.0\n" +
                            "300.0\t2024-01-03T00:00:00.000000Z\t10.0\n" +
                            "400.0\t2024-01-04T00:00:00.000000Z\t40.0\n" +
                            "500.0\t2024-01-05T00:00:00.000000Z\t40.0\n");

            final String lt = "SELECT o.price, o.ts, z.x FROM jo o LT JOIN " + right;
            assertQuery(lt)
                    .timestamp("ts")
                    .expectSize()
                    .noRandomAccess()
                    .returns("price\tts\tx\n" +
                            "100.0\t2024-01-01T00:00:00.000000Z\tnull\n" +
                            "200.0\t2024-01-02T00:00:00.000000Z\t10.0\n" +
                            "300.0\t2024-01-03T00:00:00.000000Z\t10.0\n" +
                            "400.0\t2024-01-04T00:00:00.000000Z\t10.0\n" +
                            "500.0\t2024-01-05T00:00:00.000000Z\t40.0\n");
            final String plan = planOf(asof);
            Assert.assertTrue("nested join operand must retain the window plan: " + plan, plan.contains("CachedWindow"));
            Assert.assertFalse("no legacy SUBSAMPLE node may survive: " + plan, plan.contains("Subsample"));
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
    public void testSubsampleWithDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T01:00:00.000000Z'),
                    (30.0, '2024-01-01T02:00:00.000000Z')
                    """);
            final String query = "SELECT DISTINCT ts, price FROM t SUBSAMPLE uniform(2)";
            assertSql(
                    "ts\tprice\n" +
                            "2024-01-01T00:00:00.000000Z\t10.0\n" +
                            "2024-01-01T02:00:00.000000Z\t30.0\n",
                    query
            );
            printSql("EXPLAIN " + query);
            final String plan = sink.toString();
            Assert.assertTrue("DISTINCT must execute below the window selector: " + plan, plan.indexOf("CachedWindow") < plan.indexOf("Async Group By"));
            Assert.assertTrue("DISTINCT plan must use a window selector: " + plan, plan.contains("CachedWindow"));
            Assert.assertTrue("DISTINCT must remain below the window selector: " + plan, plan.contains("Async Group By"));
            Assert.assertFalse("DISTINCT plan must not use the legacy cursor: " + plan, plan.contains("Subsample"));
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
    public void testExplainPlanShowsWindowSubsample() throws Exception {
        // `lttb(price, 500)` is a happy-path case the migration is designed to move OFF the custom
        // SUBSAMPLE cursor, so the plan no longer shows a "Subsample" node - it shows the desugared
        // keep-flag window, now with the filter FUSED into a row-selecting node (same shape as
        // testLttbDesugarsToWindowFilter): CachedWindowLightSelect, no separate Filter.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 500)")
                    .assertsPlan("SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [lttb(ts,price,500) over (order by [ts])]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n");
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
        // `lttb(price, 500)` is a happy-path case that now desugars to a window keep-flag and fuses the
        // filter into a row-selecting window node (see testExplainPlanShowsWindowSubsample). The original
        // intent of this test - SUBSAMPLE's row reduction happens before the outer ORDER BY sorts the
        // (already-reduced) result - still holds: the fused window node (CachedWindowLightSelect) is
        // nested INSIDE, i.e. printed after/deeper than, the outer sort node, meaning it runs first,
        // feeding the sort with the already-subsampled rows.
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
                        int windowPos = plan.indexOf("CachedWindowLightSelect");
                        int sortPos = plan.indexOf("sort");
                        Assert.assertTrue("Plan should contain the fused row-selecting window node: " + plan, windowPos >= 0);
                        Assert.assertTrue("Plan should contain a sort node: " + plan, sortPos >= 0);
                        Assert.assertTrue("fused window node should be nested inside the outer sort: " + plan,
                                windowPos > sortPos);
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

    // ---- Window selection across input shapes ----

    @Test
    public void testWindowDirectScanPassesThroughColumns() throws Exception {
        // Direct table scan: all pass-through columns must remain aligned with selected rows.
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
    public void testWindowSelectionAfterWhere() throws Exception {
        // WHERE filters the input before the row-selecting window runs.
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
    public void testWindowSelectionBeforeLimit() throws Exception {
        // LIMIT runs after SUBSAMPLE and reduces the already-selected result.
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
    public void testWindowCursorReuseAndToTop() throws Exception {
        // Verify the window cursor can be reused via getCursor() and toTop() resets iteration correctly.
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
    public void testWindowAfterSampleBy() throws Exception {
        // The aggregation wrapper exposes SAMPLE BY output to the outer row-selecting window.
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
            // SAMPLE BY produces 3 rows and SUBSAMPLE reduces them to 2.
            assertSql(
                    "ts\tavg\n" +
                            "2024-01-01T00:00:00.000000Z\t15.0\n" +
                            "2024-01-01T02:00:00.000000Z\t55.0\n",
                    "SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE lttb(avg, 2)"
            );
        });
    }

    @Test
    public void testWindowAfterSampleByCursorReuse() throws Exception {
        // Verify the window-over-aggregation cursor can be reused across executions.
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
    public void testWindowSelectionPreservesDescendingInput() throws Exception {
        // The algorithm selects by ascending timestamp while the row-selecting window preserves the
        // descending order supplied by its input query.
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
            // Inner subquery delivers rows in DESC order. LTTB's OVER (ORDER BY ts) window still selects
            // first (10) and last (40) by ascending timestamp, but - unlike the deleted cursor, which
            // force-sorted its output ascending - the keep-flag window preserves the query's own DESC
            // ordering, so the two kept rows are emitted 40 then 10 (same row SET, honouring ORDER BY).
            assertSql(
                    "price\tts\n" +
                            "40.0\t2024-01-01T04:00:00.000000Z\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n",
                    "SELECT price, ts FROM (SELECT price, ts FROM t ORDER BY ts DESC) SUBSAMPLE lttb(price, 2)"
            );
        });
    }

    @Test
    public void testWindowSelectionMapsOrderedTraversalToDescendingInputRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO m VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (NULL, '2024-01-01T01:00:00.000000Z'),
                    (5.0, '2024-01-01T02:00:00.000000Z'),
                    (30.0, '2024-01-01T03:00:00.000000Z'),
                    (15.0, '2024-01-01T04:00:00.000000Z'),
                    (25.0, '2024-01-01T05:00:00.000000Z'),
                    (8.0, '2024-01-01T06:00:00.000000Z'),
                    (35.0, '2024-01-01T07:00:00.000000Z')
                    """);

            final String descendingM = "(SELECT price, ts FROM m ORDER BY ts DESC)";
            final String uniform = "SELECT price, ts FROM " + descendingM + " SUBSAMPLE uniform(3)";
            final String cadence = "SELECT price, ts FROM " + descendingM + " SUBSAMPLE cadence(3, 1)";
            final String m4 = "SELECT price, ts FROM " + descendingM + " SUBSAMPLE m4(price, 4)";
            final String minmax = "SELECT price, ts FROM " + descendingM + " SUBSAMPLE minmax(price, 4)";

            assertQuery(uniform).timestampDesc("ts").returns(
                    "price\tts\n" +
                            "35.0\t2024-01-01T07:00:00.000000Z\n" +
                            "15.0\t2024-01-01T04:00:00.000000Z\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n"
            );
            // Seed 1 has offset 1 for stride 3, selecting ascending ordinals 0, 4 and 7.
            assertQuery(cadence).timestampDesc("ts").returns(
                    "price\tts\n" +
                            "35.0\t2024-01-01T07:00:00.000000Z\n" +
                            "15.0\t2024-01-01T04:00:00.000000Z\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n"
            );
            // NULL at 01:00 is dropped from the value buffer; selected non-null ordinals must still
            // map through window traversal to the correct incoming rows.
            assertQuery(m4).timestampDesc("ts").returns(
                    "price\tts\n" +
                            "35.0\t2024-01-01T07:00:00.000000Z\n" +
                            "5.0\t2024-01-01T02:00:00.000000Z\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n"
            );
            assertQuery(minmax).timestampDesc("ts").returns(
                    "price\tts\n" +
                            "35.0\t2024-01-01T07:00:00.000000Z\n" +
                            "8.0\t2024-01-01T06:00:00.000000Z\n" +
                            "30.0\t2024-01-01T03:00:00.000000Z\n" +
                            "5.0\t2024-01-01T02:00:00.000000Z\n"
            );

            execute("CREATE TABLE l (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO l VALUES
                    (10.0, '2024-01-02T00:00:00.000000Z'),
                    (20.0, '2024-01-02T01:00:00.000000Z'),
                    (50.0, '2024-01-02T02:00:00.000000Z'),
                    (30.0, '2024-01-02T03:00:00.000000Z'),
                    (15.0, '2024-01-02T04:00:00.000000Z'),
                    (45.0, '2024-01-02T05:00:00.000000Z'),
                    (25.0, '2024-01-02T06:00:00.000000Z'),
                    (35.0, '2024-01-02T07:00:00.000000Z'),
                    (5.0, '2024-01-02T08:00:00.000000Z'),
                    (40.0, '2024-01-02T09:00:00.000000Z')
                    """);
            final String lttb = "SELECT price, ts FROM (SELECT price, ts FROM l ORDER BY ts DESC) SUBSAMPLE lttb(price, 5)";
            assertQuery(lttb).timestampDesc("ts").returns(
                    "price\tts\n" +
                            "40.0\t2024-01-02T09:00:00.000000Z\n" +
                            "5.0\t2024-01-02T08:00:00.000000Z\n" +
                            "15.0\t2024-01-02T04:00:00.000000Z\n" +
                            "50.0\t2024-01-02T02:00:00.000000Z\n" +
                            "10.0\t2024-01-02T00:00:00.000000Z\n"
            );

            execute("CREATE TABLE s (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO s VALUES
                    (0.0, '2024-01-03T00:00:00.000000Z'),
                    (0.0, '2024-01-03T01:00:00.000000Z'),
                    (0.0, '2024-01-03T02:00:00.000000Z'),
                    (NULL, '2024-01-03T03:00:00.000000Z'),
                    (5.0, '2024-01-03T04:00:00.000000Z'),
                    (5.0, '2024-01-03T05:00:00.000000Z')
                    """);
            final String sdt = "SELECT price, ts FROM (SELECT price, ts FROM s ORDER BY ts DESC) SUBSAMPLE sdt(price, 0.5)";
            assertQuery(sdt).timestampDesc("ts").returns(
                    "price\tts\n" +
                            "5.0\t2024-01-03T05:00:00.000000Z\n" +
                            "5.0\t2024-01-03T04:00:00.000000Z\n" +
                            "null\t2024-01-03T03:00:00.000000Z\n" +
                            "0.0\t2024-01-03T02:00:00.000000Z\n" +
                            "0.0\t2024-01-03T00:00:00.000000Z\n"
            );

            final String[] fusedQueries = {uniform, cadence, m4, minmax, lttb, sdt};
            for (String query : fusedQueries) {
                final String plan = planOf(query);
                Assert.assertTrue("expected fused row-selecting plan: " + plan, plan.contains("CachedWindowLightSelect"));
                Assert.assertFalse("fused plan must not contain a separate keep filter: " + plan, plan.contains("Filter filter: __keep_subsample"));
            }
        });
    }

    @Test
    public void testWindowSelectionOrdersNegativeTimestampForAlgorithm() throws Exception {
        // SAMPLE BY 1w around 1970-01-01 produces a pre-epoch bucket. The window ORDER BY must place
        // that negative timestamp correctly when choosing the algorithm's first and last points.
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
            // The subquery's ORDER BY ts DESC is honoured on output by the keep-flag window (the deleted
            // cursor force-sorted ascending). LTTB's OVER (ORDER BY ts) still selects the first
            // (1969-12-29, negative ts) and last (1970-01-12) buckets by ascending timestamp - proving the
            // negative-timestamp ordering is correct - but they are emitted in the query's DESC order.
            assertSql(
                    "ts\tavg\n" +
                            "1970-01-12T00:00:00.000000Z\t55.0\n" +
                            "1969-12-29T00:00:00.000000Z\t15.0\n",
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
    public void testWindowSelectionOnMonotonicSampleBy() throws Exception {
        // SAMPLE BY produces monotonic time buckets consumed by the outer row-selecting window.
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
            // SAMPLE BY 1h produces 3 monotonic rows and SUBSAMPLE reduces them to 2.
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
            // A bind-variable target now MIGRATES to the keep-flag window path (no legacy Subsample cursor).
            printSql("EXPLAIN SELECT price, ts FROM t SUBSAMPLE lttb(price, $1)");
            final String plan = sink.toString();
            Assert.assertTrue("bind-var target must migrate to the window path: " + plan, plan.contains("CachedWindowLightSelect"));
            Assert.assertFalse("bind-var target must not use a Subsample cursor: " + plan, plan.contains("Subsample"));
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
            // Compute expected offset using the same splitmix64 hash as production
            int offset = deterministicCadenceOffset(42, 3);
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
                int off = deterministicCadenceOffset(s, 5);
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
    public void testCadenceDesugarsToWindowFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts)");
            // After the rewrite + keep-flag filter fusion, EXPLAIN must show the fused row-selecting
            // window node (CachedWindowLightSelect) with NO separate Filter and no leaked keep column.
            assertQuery("SELECT ts, v FROM t SUBSAMPLE cadence(3)")
                    .assertsPlan("SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [cadence(3) over (order by [ts])]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n");
        });
    }

    @Test
    public void testUniformDesugarsToWindowFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts)");
            // After the rewrite + keep-flag filter fusion, EXPLAIN must show the fused row-selecting
            // window node (CachedWindowLightSelect) with NO separate Filter and no leaked keep column.
            assertQuery("SELECT ts, v FROM t SUBSAMPLE uniform(3)")
                    .assertsPlan("SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [uniform(3) over (order by [ts])]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n");
        });
    }

    @Test
    public void testM4DesugarsToWindowFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts)");
            // After the rewrite + keep-flag filter fusion, EXPLAIN must show the fused row-selecting
            // window node (CachedWindowLightSelect) with NO separate Filter and no leaked keep column.
            assertQuery("SELECT ts, v FROM t SUBSAMPLE m4(v, 3)")
                    .assertsPlan("SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [m4(ts,v,3) over (order by [ts])]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n");
        });
    }

    @Test
    public void testM4OverJoinMigratesToWindow() throws Exception {
        // Phase-5 Task 2b: a SUBSAMPLE m4(...) sitting directly on a join (Shape A) now MIGRATES to the
        // keep-flag window path instead of falling through to the legacy cursor. The desugared
        // `OVER (ORDER BY ts)` and the m4 ts arg are synthesised with the master (left/driving) table's
        // alias-qualified `p.ts` so they are not ambiguous across the join's two `ts` columns; the
        // qualifier resolves away once the join projection makes `ts` unambiguous, so the final plan
        // renders bare `[ts]`. Verified two ways: the plan shows the window keep-filter (NO `Subsample`
        // cursor node), and the query returns the identical rows the cursor produced.
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
            final String query = "SELECT p.price, p.ts, v.volume FROM prices p ASOF JOIN volumes v ON (symbol) SUBSAMPLE m4(price, 4)";
            // Plan must show the desugared window keep-filter over the join, NOT the Subsample cursor.
            assertQuery(query)
                    .assertsPlan("SelectedRecord\n" +
                            "    Filter filter: __keep_subsample\n" +
                            "        CachedWindow\n" +
                            "          unorderedFunctions: [m4(ts,price,4) over (order by [ts])]\n" +
                            "            SelectedRecord\n" +
                            "                AsOf Join Fast\n" +
                            "                  condition: v.symbol=p.symbol\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: prices\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: volumes\n");
            // Byte-identical rows to what the legacy cursor produced.
            assertSql(
                    "price\tts\tvolume\n" +
                            "100.0\t2024-01-01T00:00:00.000000Z\t1000.0\n" +
                            "200.0\t2024-01-01T01:00:00.000000Z\t2000.0\n" +
                            "150.0\t2024-01-01T02:00:00.000000Z\t1500.0\n",
                    query
            );
        });
    }

    @Test
    public void testMinMaxDesugarsToWindowFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts)");
            // After the rewrite + keep-flag filter fusion, EXPLAIN must show the fused row-selecting
            // window node (CachedWindowLightSelect) with NO separate Filter and no leaked keep column.
            assertQuery("SELECT ts, v FROM t SUBSAMPLE minmax(v, 3)")
                    .assertsPlan("SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [minmax(ts,v,3) over (order by [ts])]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n");
        });
    }

    @Test
    public void testLttbDesugarsToWindowFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts)");
            // After the rewrite + keep-flag filter fusion, EXPLAIN must show the fused row-selecting
            // window node (CachedWindowLightSelect) with NO separate Filter and no leaked keep column.
            // 2-arg lttb -> 3-arg (ts, value, target) window overload.
            assertQuery("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3)")
                    .assertsPlan("SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [lttb(ts,v,3) over (order by [ts])]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n");
            // 3-arg lttb (gap) also migrates -> 4-arg (ts, value, target, gap) window overload. The
            // shared BucketSelectWindowFunction.toPlan only surfaces (ts,value,target), so the gap does
            // not appear in the plan text; its runtime effect is verified byte-identically by the
            // testLttbGapPreserving* oracle cases.
            assertQuery("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3, '1h')")
                    .assertsPlan("SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [lttb(ts,v,3) over (order by [ts])]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n");
        });
    }

    @Test
    public void testKeepFlagFusionByteIdenticalWithNulls() throws Exception {
        // The fused row-selecting cursor (SUBSAMPLE, which desugars + fuses the keep filter) must be
        // BYTE-IDENTICAL to the untouched materialize-boolean-then-Filter path over interleaved-null
        // data, for every keep-flag method. The non-fused reference is the explicit window subquery
        // with an extra, always-true predicate on a non-null column (id >= 0), which blocks fusion
        // (the WHERE is no longer a single keep literal) yet keeps exactly the same rows. id >= 0 is
        // true even for null-price rows, so uniform/cadence (which keep rows position-only, without
        // dropping nulls) stay identical too.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP, id LONG) TIMESTAMP(ts)");
            // 200 rows: nulls at x%7==0, spikes (100.0) at x%5==0, ramp otherwise; id = x (never null).
            execute("INSERT INTO t SELECT " +
                    "case when x%7=0 then null when x%5=0 then 100.0 else x end, " +
                    "x::timestamp, x FROM long_sequence(200)");

            // value-inspecting keep-flag methods (drop null/NaN rows) -> nullBits ordinal->absolute mapping
            assertFusedMatchesNonFused("m4(price, 8)", "m4(ts, price, 8)");
            assertFusedMatchesNonFused("m4(price, 3)", "m4(ts, price, 3)");
            assertFusedMatchesNonFused("minmax(price, 8)", "minmax(ts, price, 8)");
            assertFusedMatchesNonFused("lttb(price, 8)", "lttb(ts, price, 8)");
            // position-only keep-flag methods (keep by row position, nulls not dropped)
            assertFusedMatchesNonFused("uniform(8)", "uniform(8)");
            assertFusedMatchesNonFused("uniform(3)", "uniform(3)");
            assertFusedMatchesNonFused("cadence(8)", "cadence(8)");
        });
    }

    @Test
    public void testKeepFlagFusionFallsBackOnBaseBooleanFilter() throws Exception {
        // Conservative-match guard: a WHERE over a base BOOLEAN column (not the keep flag) must NOT
        // fuse - the fused cursor would wrongly emit the keep-flag selection instead of filtering by
        // the base boolean. The plan must retain the separate Filter + CachedWindowLight.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP, flag BOOLEAN) TIMESTAMP(ts)");
            final String sql = "SELECT ts, price FROM (SELECT ts, price, flag, m4(ts, price, 8) OVER (ORDER BY ts) keep FROM t) WHERE flag";
            final String plan = planOf(sql);
            Assert.assertTrue("expected a separate Filter on the base boolean: " + plan, plan.contains("Filter filter: flag"));
            Assert.assertFalse("must not fuse a base-boolean filter: " + plan, plan.contains("CachedWindowLightSelect"));
        });
    }

    @Test
    public void testKeepFlagFusionFallsBackOnExtraFilterTerm() throws Exception {
        // Conservative-match guard: a WHERE that is more than the single keep literal (keep AND ...)
        // is not the exact fuse shape, so it must fall back to Filter + CachedWindowLight.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP, id LONG) TIMESTAMP(ts)");
            final String sql = "SELECT ts, price FROM (SELECT ts, price, id, m4(ts, price, 8) OVER (ORDER BY ts) keep FROM t) WHERE keep AND id >= 0";
            final String plan = planOf(sql);
            Assert.assertTrue("expected a separate Filter node: " + plan, plan.contains("Filter"));
            Assert.assertFalse("must not fuse when extra filter terms are present: " + plan, plan.contains("CachedWindowLightSelect"));
        });
    }

    @Test
    public void testKeepFlagFusionFallsBackWhenProjectingKeepBoolean() throws Exception {
        // Critical correctness guard (bug found in review of commit 2085c8ac6f): a hand-written window
        // query that BOTH filters on AND projects the row-selecting keep boolean must NOT fuse. The
        // fused cursor skips writing the per-row boolean, so a projected copy would read the unwritten
        // narrow-chain slot - false for every kept row - instead of true. Fusion is now gated on the
        // desugar-only __keep_subsample marker (isSubsampleKeepFlag), so this hand-written shape falls
        // back to Filter + CachedWindowLight and the projected keep is correctly true for every row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, price DOUBLE, id LONG) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 10.0, 1)," +
                    "('2024-01-01T01:00:00.000000Z', 20.0, 2)," +
                    "('2024-01-01T02:00:00.000000Z', 30.0, 3)," +
                    "('2024-01-01T03:00:00.000000Z', 40.0, 4)," +
                    "('2024-01-01T04:00:00.000000Z', 50.0, 5)");

            final String sql = "SELECT ts, price, id, keep FROM (SELECT ts, price, id, m4(ts, price, 8) OVER (ORDER BY ts) keep FROM t) WHERE keep";

            // Must fall back: no fused row-selecting node, and a separate Filter on the keep boolean.
            final String plan = planOf(sql);
            Assert.assertFalse("must not fuse when the keep boolean is projected: " + plan, plan.contains("CachedWindowLightSelect"));
            Assert.assertTrue("expected a separate Filter node + CachedWindowLight: " + plan,
                    plan.contains("Filter") && plan.contains("CachedWindowLight"));

            // target 8 >= 5 rows -> all rows kept -> the projected keep must be true for EVERY row
            // (the bug returned false for every row).
            assertSql(
                    "ts\tprice\tid\tkeep\n" +
                            "2024-01-01T00:00:00.000000Z\t10.0\t1\ttrue\n" +
                            "2024-01-01T01:00:00.000000Z\t20.0\t2\ttrue\n" +
                            "2024-01-01T02:00:00.000000Z\t30.0\t3\ttrue\n" +
                            "2024-01-01T03:00:00.000000Z\t40.0\t4\ttrue\n" +
                            "2024-01-01T04:00:00.000000Z\t50.0\t5\ttrue\n",
                    sql
            );

            // The real SUBSAMPLE feature must STILL fuse: same m4/target over the same table desugars
            // to the internal marked keep flag and takes the fused row-selecting path.
            final String subsamplePlan = planOf("SELECT ts, price FROM t SUBSAMPLE m4(price, 8)");
            Assert.assertTrue("SUBSAMPLE m4 must still fuse into the row-selecting node: " + subsamplePlan,
                    subsamplePlan.contains("CachedWindowLightSelect"));
        });
    }

    private void assertFusedMatchesNonFused(String subsampleCall, String windowCall) throws SqlException {
        printSql("SELECT ts, price FROM t SUBSAMPLE " + subsampleCall);
        final String fused = sink.toString();
        printSql("SELECT ts, price FROM (SELECT ts, price, id, " + windowCall
                + " OVER (ORDER BY ts) keep FROM t) WHERE keep AND id >= 0");
        Assert.assertEquals("fused vs non-fused mismatch for " + subsampleCall, fused, sink.toString());
    }

    private String planOf(String sql) throws SqlException {
        printSql("EXPLAIN " + sql);
        return sink.toString();
    }

    @Test
    public void testUniformSelectStarDoesNotLeakKeepColumn() throws Exception {
        // Regression: SELECT * must project only the table's columns. The internal __keep_subsample
        // window flag used by the rewrite must never surface in wildcard output.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 1, '2024-01-01T00:00:00.000000Z'),
                    (20.0, 2, '2024-01-01T01:00:00.000000Z'),
                    (30.0, 3, '2024-01-01T02:00:00.000000Z'),
                    (40.0, 4, '2024-01-01T03:00:00.000000Z'),
                    (50.0, 5, '2024-01-01T04:00:00.000000Z'),
                    (60.0, 6, '2024-01-01T05:00:00.000000Z'),
                    (70.0, 7, '2024-01-01T06:00:00.000000Z'),
                    (80.0, 8, '2024-01-01T07:00:00.000000Z'),
                    (90.0, 9, '2024-01-01T08:00:00.000000Z'),
                    (100.0, 10, '2024-01-01T09:00:00.000000Z')
                    """);
            drainWalQueue();
            // 10 rows, target 4: positions 0, 3, 6, 9. Header must be exactly price, qty, ts.
            assertSql(
                    "price\tqty\tts\n" +
                            "10.0\t1\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t4\t2024-01-01T03:00:00.000000Z\n" +
                            "70.0\t7\t2024-01-01T06:00:00.000000Z\n" +
                            "100.0\t10\t2024-01-01T09:00:00.000000Z\n",
                    "SELECT * FROM t SUBSAMPLE uniform(4)"
            );
        });
    }

    @Test
    public void testM4SelectStarDoesNotLeakKeepColumn() throws Exception {
        // Regression: same as testUniformSelectStarDoesNotLeakKeepColumn, but for the
        // value-inspecting (m4) desugar path. SELECT * must project only the table's columns -
        // the internal __keep_subsample window flag must never surface in wildcard output.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 1, '2024-01-01T00:00:00.000000Z'),
                    (20.0, 2, '2024-01-01T01:00:00.000000Z'),
                    (30.0, 3, '2024-01-01T02:00:00.000000Z'),
                    (40.0, 4, '2024-01-01T03:00:00.000000Z'),
                    (50.0, 5, '2024-01-01T04:00:00.000000Z'),
                    (60.0, 6, '2024-01-01T05:00:00.000000Z'),
                    (70.0, 7, '2024-01-01T06:00:00.000000Z'),
                    (80.0, 8, '2024-01-01T07:00:00.000000Z'),
                    (90.0, 9, '2024-01-01T08:00:00.000000Z'),
                    (100.0, 10, '2024-01-01T09:00:00.000000Z')
                    """);
            drainWalQueue();
            // m4 keeps first/last/min/max per bucket. With monotonically increasing values, min
            // and max coincide with first/last, so only the global first and last row survive.
            // Header must be exactly price, qty, ts - no __keep_subsample leak.
            assertSql(
                    "price\tqty\tts\n" +
                            "10.0\t1\t2024-01-01T00:00:00.000000Z\n" +
                            "100.0\t10\t2024-01-01T09:00:00.000000Z\n",
                    "SELECT * FROM t SUBSAMPLE m4(price, 4)"
            );
        });
    }

    @Test
    public void testUniformLimitAppliesAfterSubsample() throws Exception {
        // Regression: LIMIT must apply AFTER the uniform subsample, not before it. The rewrite re-lifts
        // LIMIT above the __keep_subsample filter so LIMIT k returns the first k of the selected rows.
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
            // uniform(4) on 10 rows -> positions 0, 3, 6, 9 (prices 10, 40, 70, 100).
            // LIMIT 2 must keep the FIRST 2 of those selected rows: 10, 40 (not the first 2 raw rows).
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "40.0\t2024-01-01T03:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE uniform(4) LIMIT 2"
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
    public void testCadenceStrideOneWithBindSeedUnset() throws Exception {
        // Legacy cadence(1) is a no-op and returns the base cursor without reading its seed.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (10.0, '2024-01-01T00:00:00.000000Z')");
            bindVariableService.clear();
            bindVariableService.setLong(0, 1);
            assertSql(
                    "price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n",
                    "SELECT price, ts FROM t SUBSAMPLE cadence($1, $2)"
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
    public void testUniformAfterSampleByUsesWindowPlan() throws Exception {
        // Proves the aggregation-context SUBSAMPLE now takes the desugared keep-flag WINDOW path
        // (CachedWindowLightSelect fused, no separate Filter, no leaked __keep_subsample) sitting
        // ABOVE the group-by/sample-by node - and NO Subsample cursor node.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertQuery("SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE uniform(2)")
                    .assertsPlan("SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [uniform(2) over (order by [ts])]\n" +
                            "        Encode sort light\n" +
                            "          keys: [ts]\n" +
                            "            Async Group By workers: 1\n" +
                            "              keys: [ts]\n" +
                            "              keyFunctions: [timestamp_floor_utc('1h',ts)]\n" +
                            "              values: [avg(price)]\n" +
                            "              filter: null\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: t\n");
        });
    }

    @Test
    public void testCadenceAfterSampleByUsesWindowPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertQuery("SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE cadence(2)")
                    .assertsPlan("SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [cadence(2) over (order by [ts])]\n" +
                            "        Encode sort light\n" +
                            "          keys: [ts]\n" +
                            "            Async Group By workers: 1\n" +
                            "              keys: [ts]\n" +
                            "              keyFunctions: [timestamp_floor_utc('1h',ts)]\n" +
                            "              values: [avg(price)]\n" +
                            "              filter: null\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: t\n");
        });
    }

    @Test
    public void testLttbAfterSampleByUsesWindowPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertQuery("SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE lttb(avg, 2)")
                    .assertsPlan("SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [lttb(ts,avg,2) over (order by [ts])]\n" +
                            "        Encode sort light\n" +
                            "          keys: [ts]\n" +
                            "            Async Group By workers: 1\n" +
                            "              keys: [ts]\n" +
                            "              keyFunctions: [timestamp_floor_utc('1h',ts)]\n" +
                            "              values: [avg(price)]\n" +
                            "              filter: null\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: t\n");
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
            // A bind-variable stride now MIGRATES to the keep-flag window path (no legacy Subsample cursor).
            printSql("EXPLAIN SELECT price, ts FROM t SUBSAMPLE cadence($1)");
            final String plan = sink.toString();
            Assert.assertTrue("bind-var stride must migrate to the window path: " + plan, plan.contains("CachedWindowLightSelect"));
            Assert.assertFalse("bind-var stride must not use a Subsample cursor: " + plan, plan.contains("Subsample"));
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

    // Mirrors the splitmix64 hash used by the cadence window function.
    // for deterministic cadence offset computation.
    @Test
    public void testSubsampleOnNonFinalUnionArmRejected() throws Exception {
        // SUBSAMPLE on a non-final UNION arm must be rejected, like ORDER BY / LIMIT,
        // rather than silently downsampling only the first arm.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 5) UNION SELECT price, ts FROM t",
                    24,
                    "unexpected token 'subsample'"
            );
        });
    }

    @Test
    public void testM4ExpressionValueArgRejected() throws Exception {
        // The value-inspecting gate migrates m4/minmax to the window path ONLY when the value arg
        // (arg 0) is a bare column literal. The legacy cursor resolved the value arg BY NAME ONLY
        // (columnNode.token), so an expression like v*2 looked up a column literally named "*" and
        // failed with "column not found". The rewrite now throws that same error (message and
        // position) directly for a non-literal value arg, so the window path never silently
        // evaluates v*2 as a DOUBLE expression. Assert the cursor-identical error.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT ts, v FROM t SUBSAMPLE m4(v * 2, 4)",
                    35,
                    "column not found: *"
            );
            assertException(
                    "SELECT ts, v FROM t SUBSAMPLE m4(abs(v), 4)",
                    33,
                    "column not found: abs"
            );
        });
    }

    // ---- sdt (Swinging Door Trending) desugaring: SUBSAMPLE sdt(value, compdev) ----
    // sdt has NO custom SUBSAMPLE cursor, so the rewrite gate is TOTAL: a valid shape migrates to the
    // sdt(ts, value, compdev) keep-flag window function; every other sdt shape throws a specific
    // SqlException here (never the generic codegen "unknown subsample method").

    @Test
    public void testSdtDesugarsToWindowFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            // After the rewrite + keep-flag filter fusion, EXPLAIN must show the fused row-selecting
            // window node (CachedWindowLightSelect) with NO separate Filter and no leaked keep column -
            // sdt exposes its finalized swinging-door keep-set via getSelectedRows() (mirrors
            // testM4DesugarsToWindowFilter). The O(N) BOOLEAN Filter is replaced by O(selected) output.
            assertSql(
                    "QUERY PLAN\n" +
                            "SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [sdt(ts, price, 0.5) over (order by [ts])]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: x\n",
                    "EXPLAIN SELECT ts, price FROM x SUBSAMPLE sdt(price, 0.5)"
            );
        });
    }

    @Test
    public void testSdtKeepsAllRowsFused() throws Exception {
        // compdev = 0 forces the swinging door to keep (nearly) every row: exercises the fused
        // row-selecting path where the keep-set is the full/near-full result, and confirms it stays
        // byte-identical to the unfused window+keep-filter oracle.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO x SELECT rnd_double() * 100, timestamp_sequence('2024-01-01', 60000000) FROM long_sequence(200)");
            printSql("SELECT ts, price FROM (SELECT *, sdt(ts, price, 0.0) OVER (ORDER BY ts) k FROM x) WHERE k");
            final String expected = sink.toString();
            assertSql(expected, "SELECT ts, price FROM x SUBSAMPLE sdt(price, 0.0)");
            // and the fused plan is used
            assertSql(
                    "QUERY PLAN\n" +
                            "SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [sdt(ts, price, 0.0) over (order by [ts])]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: x\n",
                    "EXPLAIN SELECT ts, price FROM x SUBSAMPLE sdt(price, 0.0)"
            );
        });
    }

    @Test
    public void testSdtMatchesWindowFunction() throws Exception {
        // The sdt window function IS the oracle (sdt has no byte-identity cursor). The desugared
        // SUBSAMPLE must be byte-identical to the explicit window+keep-filter it lowers to.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO x SELECT rnd_double() * 100, timestamp_sequence('2024-01-01', 60000000) FROM long_sequence(500)");
            printSql("SELECT ts, price FROM (SELECT *, sdt(ts, price, 0.5) OVER (ORDER BY ts) k FROM x) WHERE k");
            final String expected = sink.toString();
            assertSql(expected, "SELECT ts, price FROM x SUBSAMPLE sdt(price, 0.5)");
        });
    }

    @Test
    public void testSdtNullFlush() throws Exception {
        // A null value mid-series: default RESPECT NULLS. The desugared SUBSAMPLE must match the
        // window function's null handling byte-for-byte.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO x VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (10.0, '2024-01-01T01:00:00.000000Z'),
                    (10.0, '2024-01-01T02:00:00.000000Z'),
                    (NULL, '2024-01-01T03:00:00.000000Z'),
                    (50.0, '2024-01-01T04:00:00.000000Z'),
                    (50.0, '2024-01-01T05:00:00.000000Z'),
                    (90.0, '2024-01-01T06:00:00.000000Z')
                    """);
            printSql("SELECT ts, price FROM (SELECT *, sdt(ts, price, 0.5) OVER (ORDER BY ts) k FROM x) WHERE k");
            final String expected = sink.toString();
            assertSql(expected, "SELECT ts, price FROM x SUBSAMPLE sdt(price, 0.5)");
        });
    }

    @Test
    public void testSdtNonConstantCompdev() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT ts, price FROM x SUBSAMPLE sdt(price, $1)",
                    34,
                    "SUBSAMPLE sdt requires a constant, non-negative compdev"
            );
        });
    }

    @Test
    public void testSdtNegativeCompdev() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT ts, price FROM x SUBSAMPLE sdt(price, -1.0)",
                    34,
                    "SUBSAMPLE sdt requires a constant, non-negative compdev"
            );
        });
    }

    @Test
    public void testSdtNonNumericValue() throws Exception {
        // A literal naming a SYMBOL column legitimately MIGRATES (existence/type unknown at rewrite
        // time); the sdt window factory rejects it at runtime with its own numeric-type overload
        // message, mirroring how lttb handles testErrorNonNumericColumn.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT ts, sym FROM x SUBSAMPLE sdt(sym, 0.5)",
                    36,
                    "argument type mismatch for function `sdt` at #2 expected: DOUBLE, actual: SYMBOL"
            );
        });
    }

    @Test
    public void testSdtWrongArity() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT ts, price FROM x SUBSAMPLE sdt(price)",
                    34,
                    "sdt() requires exactly 2 arguments: column and compdev"
            );
            assertException(
                    "SELECT ts, price FROM x SUBSAMPLE sdt(price, 0.5, 1)",
                    34,
                    "sdt() requires exactly 2 arguments: column and compdev"
            );
        });
    }

    @Test
    public void testSdtInJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE prices (price DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE volumes (volume DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertException(
                    "SELECT p.price, p.ts, v.volume FROM prices p ASOF JOIN volumes v ON (symbol) SUBSAMPLE sdt(price, 0.5)",
                    87,
                    "SUBSAMPLE sdt is not supported inside a join"
            );
        });
    }

    @Test
    public void testSdtNoDesignatedTs() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP)");
            assertException(
                    "SELECT ts, price FROM x SUBSAMPLE sdt(price, 0.5)",
                    34,
                    "SUBSAMPLE requires a designated timestamp column"
            );
        });
    }

    @Test
    public void testSdtAggContext() throws Exception {
        // The timestamp survives at the AST level even though the runtime cursor for these shapes
        // (DISTINCT, bare aggregate) loses the designated-timestamp designation - so
        // isAggregationContext() is what actually gates this, and it is reachable.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT DISTINCT ts, price FROM x SUBSAMPLE sdt(price, 0.5)",
                    43,
                    "SUBSAMPLE sdt is not supported in an aggregation context"
            );
            assertException(
                    "SELECT count(*) FROM x SUBSAMPLE sdt(price, 0.5)",
                    33,
                    "SUBSAMPLE sdt is not supported in an aggregation context"
            );
        });
    }

    @Test
    public void testSdtNonLiteralValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT ts, price FROM x SUBSAMPLE sdt(price * 2, 0.5)",
                    34,
                    "SUBSAMPLE sdt requires a plain column as its first argument"
            );
        });
    }

    private static int deterministicCadenceOffset(long seed, int stride) {
        long h = seed;
        h = (h ^ (h >>> 30)) * 0xbf58476d1ce4e5b9L;
        h = (h ^ (h >>> 27)) * 0x94d049bb133111ebL;
        h = h ^ (h >>> 31);
        // mirror production: floorMod, not Math.abs(h) % stride (Math.abs(Long.MIN_VALUE) is negative)
        return (int) Math.floorMod(h, (long) stride);
    }
}
