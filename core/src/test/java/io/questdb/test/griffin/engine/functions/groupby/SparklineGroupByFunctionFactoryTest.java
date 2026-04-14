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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class SparklineGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, grp SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (NULL, 'a', '2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES (NULL, 'a', '2024-01-01T01:00:00.000000Z')");
            assertSql(
                    "sparkline\n" +
                            "\n",
                    "SELECT sparkline(val) FROM t"
            );
        });
    }

    @Test
    public void testBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (1.0, '2024-01-01T01:00:00.000000Z'),
                    (2.0, '2024-01-01T02:00:00.000000Z'),
                    (3.0, '2024-01-01T03:00:00.000000Z'),
                    (4.0, '2024-01-01T04:00:00.000000Z'),
                    (5.0, '2024-01-01T05:00:00.000000Z'),
                    (6.0, '2024-01-01T06:00:00.000000Z'),
                    (7.0, '2024-01-01T07:00:00.000000Z')
                    """);
            assertSql(
                    "sparkline\n" +
                            "\u2581\u2582\u2583\u2584\u2585\u2586\u2587\u2588\n",
                    "SELECT sparkline(val) FROM t"
            );
        });
    }

    @Test
    public void testClampAboveMax() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (100.0, '2024-01-01T02:00:00.000000Z'),
                    (200.0, '2024-01-01T03:00:00.000000Z')
                    """);
            // With max=100, the 200 value should clamp to top char
            assertSql(
                    "sparkline\n" +
                            "\u2581\u2584\u2588\u2588\n",
                    "SELECT sparkline(val, 0.0, 100.0, 4) FROM t"
            );
        });
    }

    @Test
    public void testClampBelowMin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (-50.0, '2024-01-01T00:00:00.000000Z'),
                    (0.0, '2024-01-01T01:00:00.000000Z'),
                    (50.0, '2024-01-01T02:00:00.000000Z'),
                    (100.0, '2024-01-01T03:00:00.000000Z')
                    """);
            // With min=0, the -50 value should clamp to bottom char
            assertSql(
                    "sparkline\n" +
                            "\u2581\u2581\u2584\u2588\n",
                    "SELECT sparkline(val, 0.0, 100.0, 4) FROM t"
            );
        });
    }

    @Test
    public void testConstantValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (5.0, '2024-01-01T00:00:00.000000Z'),
                    (5.0, '2024-01-01T01:00:00.000000Z'),
                    (5.0, '2024-01-01T02:00:00.000000Z')
                    """);
            // All same value -> min==max -> all top chars
            assertSql(
                    "sparkline\n" +
                            "\u2588\u2588\u2588\n",
                    "SELECT sparkline(val) FROM t"
            );
        });
    }

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertSql(
                    "sparkline\n" +
                            "\n",
                    "SELECT sparkline(val) FROM t"
            );
        });
    }

    @Test
    public void testMixedNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (NULL, '2024-01-01T01:00:00.000000Z'),
                    (7.0, '2024-01-01T02:00:00.000000Z'),
                    (NULL, '2024-01-01T03:00:00.000000Z'),
                    (3.5, '2024-01-01T04:00:00.000000Z')
                    """);
            // NULLs are skipped; remaining values 0.0, 7.0, 3.5
            // min=0, max=7, so: 0->index 0, 7->index 7, 3.5->index 3
            assertSql(
                    "sparkline\n" +
                            "\u2581\u2588\u2584\n",
                    "SELECT sparkline(val) FROM t"
            );
        });
    }

    @Test
    public void testMultipleGroups() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, grp SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, 'up', '2024-01-01T00:00:00.000000Z'),
                    (3.5, 'up', '2024-01-01T01:00:00.000000Z'),
                    (7.0, 'up', '2024-01-01T02:00:00.000000Z'),
                    (7.0, 'down', '2024-01-01T00:00:00.000000Z'),
                    (3.5, 'down', '2024-01-01T01:00:00.000000Z'),
                    (0.0, 'down', '2024-01-01T02:00:00.000000Z')
                    """);
            assertSql(
                    "grp\tsparkline\n" +
                            "down\t\u2588\u2584\u2581\n" +
                            "up\t\u2581\u2584\u2588\n",
                    "SELECT grp, sparkline(val) FROM t ORDER BY grp"
            );
        });
    }

    @Test
    public void testNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (-10.0, '2024-01-01T00:00:00.000000Z'),
                    (-5.0, '2024-01-01T01:00:00.000000Z'),
                    (0.0, '2024-01-01T02:00:00.000000Z'),
                    (5.0, '2024-01-01T03:00:00.000000Z'),
                    (10.0, '2024-01-01T04:00:00.000000Z')
                    """);
            // min=-10, max=10, range=20
            // -10->0, -5->1, 0->3, 5->5, 10->7
            assertSql(
                    "sparkline\n" +
                            "\u2581\u2582\u2584\u2586\u2588\n",
                    "SELECT sparkline(val) FROM t"
            );
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (7.0, '2024-01-01T00:30:00.000000Z'),
                    (3.5, '2024-01-01T01:00:00.000000Z'),
                    (3.5, '2024-01-01T01:30:00.000000Z')
                    """);
            assertSql(
                    "ts\tsparkline\n" +
                            "2024-01-01T00:00:00.000000Z\t\u2581\u2588\n" +
                            "2024-01-01T01:00:00.000000Z\t\u2588\u2588\n",
                    "SELECT ts, sparkline(val) FROM t SAMPLE BY 1h"
            );
        });
    }

    @Test
    public void testSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (42.0, '2024-01-01T00:00:00.000000Z')");
            // Single value -> min==max -> top char
            assertSql(
                    "sparkline\n" +
                            "\u2588\n",
                    "SELECT sparkline(val) FROM t"
            );
        });
    }

    @Test
    public void testWidthSubsampling() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (0.0, '2024-01-01T01:00:00.000000Z'),
                    (7.0, '2024-01-01T02:00:00.000000Z'),
                    (7.0, '2024-01-01T03:00:00.000000Z')
                    """);
            // 4 values sub-sampled to width=2: bucket1=avg(0,0)=0, bucket2=avg(7,7)=7
            assertSql(
                    "sparkline\n" +
                            "\u2581\u2588\n",
                    "SELECT sparkline(val, NULL, NULL, 2) FROM t"
            );
        });
    }

    @Test
    public void testWithExplicitMinMax() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (25.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (75.0, '2024-01-01T02:00:00.000000Z')
                    """);
            // With min=0, max=100: 25->index 1, 50->index 3, 75->index 5
            assertSql(
                    "sparkline\n" +
                            "\u2582\u2584\u2586\n",
                    "SELECT sparkline(val, 0.0, 100.0, 3) FROM t"
            );
        });
    }

    @Test
    public void testWithNullMinAutoMax() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (3.5, '2024-01-01T01:00:00.000000Z'),
                    (7.0, '2024-01-01T02:00:00.000000Z')
                    """);
            // NULL min -> auto (0.0), NULL max -> auto (7.0)
            assertSql(
                    "sparkline\n" +
                            "\u2581\u2584\u2588\n",
                    "SELECT sparkline(val, NULL, NULL, 3) FROM t"
            );
        });
    }

    @Test
    public void testFactoryReuse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (7.0, '2024-01-01T01:00:00.000000Z')
                    """);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (RecordCursorFactory fact = compiler.compile("SELECT sparkline(val) FROM t", sqlExecutionContext).getRecordCursorFactory()) {
                    // execute factory multiple times to verify pool reuse
                    for (int i = 0; i < 3; i++) {
                        try (RecordCursor cursor = fact.getCursor(sqlExecutionContext)) {
                            TestUtils.assertCursor(
                                    "sparkline\n\u2581\u2588\n",
                                    cursor,
                                    fact.getMetadata(),
                                    true,
                                    sink
                            );
                        }
                    }

                    // replace with larger data and re-execute
                    execute("TRUNCATE TABLE t");
                    execute("""
                            INSERT INTO t VALUES
                            (0.0, '2024-01-01T00:00:00.000000Z'),
                            (3.5, '2024-01-01T01:00:00.000000Z'),
                            (7.0, '2024-01-01T02:00:00.000000Z')
                            """);
                    for (int i = 0; i < 3; i++) {
                        try (RecordCursor cursor = fact.getCursor(sqlExecutionContext)) {
                            TestUtils.assertCursor(
                                    "sparkline\n\u2581\u2584\u2588\n",
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
    public void testIntegerColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0, '2024-01-01T00:00:00.000000Z'),
                    (3, '2024-01-01T01:00:00.000000Z'),
                    (7, '2024-01-01T02:00:00.000000Z')
                    """);
            assertSql(
                    "sparkline\n" +
                            "\u2581\u2584\u2588\n",
                    "SELECT sparkline(val) FROM t"
            );
        });
    }

    @Test
    public void testLongColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0, '2024-01-01T00:00:00.000000Z'),
                    (50, '2024-01-01T01:00:00.000000Z'),
                    (100, '2024-01-01T02:00:00.000000Z')
                    """);
            assertSql(
                    "sparkline\n" +
                            "\u2581\u2584\u2588\n",
                    "SELECT sparkline(val) FROM t"
            );
        });
    }

    @Test
    public void testShortColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val SHORT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0::SHORT, '2024-01-01T00:00:00.000000Z'),
                    (50::SHORT, '2024-01-01T01:00:00.000000Z'),
                    (100::SHORT, '2024-01-01T02:00:00.000000Z')
                    """);
            assertSql(
                    "sparkline\n" +
                            "\u2581\u2584\u2588\n",
                    "SELECT sparkline(val) FROM t"
            );
        });
    }

    @Test
    public void testWidthInvalidZero() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT sparkline(val, 0.0, 100.0, 0) FROM t",
                    34,
                    "width must be a positive integer"
            );
        });
    }

    @Test
    public void testNonConstantMinRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, bound DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT sparkline(val, bound, 100.0, 10) FROM t",
                    7,
                    "there is no matching function `sparkline` with the argument types: (DOUBLE, DOUBLE, DOUBLE, INT)"
            );
        });
    }

    @Test
    public void testMemoryLimitExceeded() throws Exception {
        // getStrFunctionMaxBufferLength() defaults to 1MB = 1_048_576 bytes.
        // Each value produces one 3-byte UTF-8 char, so maxValues = 349_525.
        // We can't easily insert 350K rows in a test, so override the config
        // to a small limit and verify the error.
        setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, 30);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (1.0, '2024-01-01T00:00:00.000000Z'),
                    (2.0, '2024-01-01T01:00:00.000000Z'),
                    (3.0, '2024-01-01T02:00:00.000000Z'),
                    (4.0, '2024-01-01T03:00:00.000000Z'),
                    (5.0, '2024-01-01T04:00:00.000000Z'),
                    (6.0, '2024-01-01T05:00:00.000000Z'),
                    (7.0, '2024-01-01T06:00:00.000000Z'),
                    (8.0, '2024-01-01T07:00:00.000000Z'),
                    (9.0, '2024-01-01T08:00:00.000000Z'),
                    (10.0, '2024-01-01T09:00:00.000000Z'),
                    (11.0, '2024-01-01T10:00:00.000000Z')
                    """);
            assertException(
                    "SELECT sparkline(val) FROM t",
                    17,
                    "sparkline() result exceeds max size of 30 bytes"
            );
        });
    }

    @Test
    public void testSampleByFillNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (7.0, '2024-01-01T00:30:00.000000Z'),
                    (3.5, '2024-01-01T02:00:00.000000Z'),
                    (3.5, '2024-01-01T02:30:00.000000Z')
                    """);
            // Hour 01:00 has no data, FILL(NULL) should produce NULL for that bucket
            assertSql(
                    "ts\tsparkline\n" +
                            "2024-01-01T00:00:00.000000Z\t\u2581\u2588\n" +
                            "2024-01-01T01:00:00.000000Z\t\n" +
                            "2024-01-01T02:00:00.000000Z\t\u2588\u2588\n",
                    "SELECT ts, sparkline(val) FROM t SAMPLE BY 1h FILL(NULL)"
            );
        });
    }

    @Test
    public void testSampleByFillPrev() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (7.0, '2024-01-01T00:30:00.000000Z'),
                    (3.5, '2024-01-01T02:00:00.000000Z'),
                    (3.5, '2024-01-01T02:30:00.000000Z')
                    """);
            // Hour 01:00 has no data, FILL(PREV) should repeat hour 00's sparkline
            assertSql(
                    "ts\tsparkline\n" +
                            "2024-01-01T00:00:00.000000Z\t\u2581\u2588\n" +
                            "2024-01-01T01:00:00.000000Z\t\u2581\u2588\n" +
                            "2024-01-01T02:00:00.000000Z\t\u2588\u2588\n",
                    "SELECT ts, sparkline(val) FROM t SAMPLE BY 1h FILL(PREV)"
            );
        });
    }
}
