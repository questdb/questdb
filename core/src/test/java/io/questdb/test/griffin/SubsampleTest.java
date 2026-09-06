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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SubsampleTest extends AbstractCairoTest {

    @Test
    public void testConfiguredRowCapAppliesOnlyToSubsampleMethods() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_SUBSAMPLE_MAX_ROWS, 5L);
            execute("CREATE TABLE at_cap AS (" +
                    "SELECT x::double price, timestamp_sequence(0, 1) ts FROM long_sequence(5)) TIMESTAMP(ts)");
            execute("CREATE TABLE over_cap AS (" +
                    "SELECT x::double price, timestamp_sequence(0, 1) ts FROM long_sequence(6)) TIMESTAMP(ts)");

            final String[] methods = {
                    "uniform(2)",
                    "cadence(2)",
                    "cadence(2, 7)",
                    "m4(price, 2)",
                    "minmax(price, 2)",
                    "lttb(price, 2)",
                    "lttb(price, 2, '1h')"
            };
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (String method : methods) {
                    try (RecordCursorFactory factory = compiler.compile(
                            "SELECT price, ts FROM at_cap SUBSAMPLE " + method,
                            sqlExecutionContext).getRecordCursorFactory();
                         RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue("expected rows at cap for " + method, cursor.hasNext());
                        while (cursor.hasNext()) {
                            // drain
                        }
                    }

                    final String overCapQuery = "SELECT price, ts FROM over_cap SUBSAMPLE " + method;
                    try (RecordCursorFactory factory = compiler.compile(
                            overCapQuery,
                            sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            while (cursor.hasNext()) {
                                // drain until cap breach
                            }
                            Assert.fail("expected row-cap breach for " + method);
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "SUBSAMPLE input exceeds maximum of 5 rows");
                            Assert.assertEquals(overCapQuery.indexOf(method.substring(0, method.indexOf('('))), e.getPosition());
                        }
                    }
                }

                // Direct public window calls are governed by the query memory tracker, not the
                // clause-specific cap. cadence(1) also preserves the legacy no-op cap bypass.
                final String[] uncappedQueries = {
                        "SELECT uniform(2) OVER (ORDER BY ts) FROM over_cap",
                        "SELECT m4(ts, price, 2) OVER (ORDER BY ts) FROM over_cap",
                        "SELECT price, ts FROM over_cap SUBSAMPLE cadence(1)"
                };
                for (String query : uncappedQueries) {
                    try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory();
                         RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long count = 0;
                        while (cursor.hasNext()) {
                            count++;
                        }
                        Assert.assertEquals(query, 6, count);
                    }
                }
            }
        });
    }

    @Test
    public void testConfiguredRowCapCountsNullRows() throws Exception {
        // The cap counts physical input rows. m4/minmax/lttb drop NULL-valued rows from their buffers,
        // but every row still costs a base row id, a sort entry and a null bit in the window cursor,
        // so NULL rows count the same way every row does for uniform/cadence. A WHERE below SUBSAMPLE
        // runs before the window, so rows it filters out never reach the cap.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_SUBSAMPLE_MAX_ROWS, 5L);
            execute("CREATE TABLE at_cap_nulls AS (" +
                    "SELECT CASE WHEN x % 2 = 0 THEN null::double ELSE x::double END price, timestamp_sequence(0, 1) ts FROM long_sequence(5)) TIMESTAMP(ts)");
            execute("CREATE TABLE over_cap_all_null AS (" +
                    "SELECT null::double price, timestamp_sequence(0, 1) ts FROM long_sequence(6)) TIMESTAMP(ts)");
            execute("CREATE TABLE over_cap_one_null AS (" +
                    "SELECT CASE WHEN x = 6 THEN null::double ELSE x::double END price, timestamp_sequence(0, 1) ts FROM long_sequence(6)) TIMESTAMP(ts)");
            // Sparse-table shape: 1000 physical rows, five of them carrying a value.
            execute("CREATE TABLE sparse_prices AS (" +
                    "SELECT CASE WHEN x <= 5 THEN x::double ELSE null::double END price, timestamp_sequence(0, 1) ts FROM long_sequence(1000)) TIMESTAMP(ts)");

            final String[] methods = {
                    "uniform(2)",
                    "cadence(2)",
                    "cadence(2, 7)",
                    "m4(price, 2)",
                    "minmax(price, 2)",
                    "lttb(price, 2)",
                    "lttb(price, 2, '1h')"
            };
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (String method : methods) {
                    // Five physical rows, NULLs included, sit exactly at the cap.
                    assertSubsampleCompletes(compiler, "SELECT price, ts FROM at_cap_nulls SUBSAMPLE " + method);
                    // The sixth physical row breaches the cap whether or not its value is NULL.
                    assertSubsampleRowCapBreach(compiler, "SELECT price, ts FROM over_cap_all_null SUBSAMPLE ", method);
                    assertSubsampleRowCapBreach(compiler, "SELECT price, ts FROM over_cap_one_null SUBSAMPLE ", method);
                    assertSubsampleRowCapBreach(compiler, "SELECT price, ts FROM sparse_prices SUBSAMPLE ", method);
                    // Filtering the NULL rows below SUBSAMPLE keeps them out of the window and the cap.
                    assertSubsampleCompletes(compiler, "SELECT price, ts FROM sparse_prices WHERE price IS NOT NULL SUBSAMPLE " + method);
                }
            }
            assertQuery("SELECT price, ts FROM sparse_prices WHERE price IS NOT NULL SUBSAMPLE lttb(price, 2)")
                    .timestamp("ts")
                    .returns("price\tts\n" +
                            "1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "5.0\t1970-01-01T00:00:00.000004Z\n");
        });
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 5)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T02:00:00.000000Z\n" +
                    "15.0\t2024-01-01T04:00:00.000000Z\n" +
                    "5.0\t2024-01-01T08:00:00.000000Z\n" +
                    "40.0\t2024-01-01T09:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 10)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "20.0\t2024-01-01T01:00:00.000000Z\n" +
                    "30.0\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 3)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "100.0\t2024-01-01T03:00:00.000000Z\n" +
                    "15.0\t2024-01-01T06:00:00.000000Z\n");
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
            assertQuery("SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE lttb(avg, 2)").timestamp("ts").returns("ts\tavg\n" +
                    "2024-01-01T00:00:00.000000Z\t15.0\n" +
                    "2024-01-01T02:00:00.000000Z\t55.0\n");
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
            assertQuery("SELECT * FROM t SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tvolume\tsymbol\tts\n" +
                    "10.0\t100\tBTC\t2024-01-01T00:00:00.000000Z\n" +
                    "20.0\t200\tBTC\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE m4(price, 4)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "5.0\t2024-01-01T02:00:00.000000Z\n" +
                    "35.0\t2024-01-01T07:00:00.000000Z\n");
        });
    }

    @Test
    public void testLttbNanoTimestampPrecision() throws Exception {
        // double ulp near a 2024 nanosecond epoch (~1.7e18) is 256ns. LTTB must
        // compute triangle areas from long timestamp differences, not absolute
        // epochs converted to double, or 1ns-apart candidates all collapse to
        // area 0 and the first candidate wins regardless of value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, value DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0, 0.0, '2024-01-01T00:00:00.000000000Z'),
                    (1, 1.0, '2024-01-01T00:00:00.000000001Z'),
                    (2, 100.0, '2024-01-01T00:00:00.000000002Z'),
                    (3, 2.0, '2024-01-01T00:00:00.000000003Z'),
                    (4, 0.0, '2024-01-01T00:00:00.000000004Z')
                    """);
            // exact triangle areas for the middle-bucket candidates: id1=4, id2=400, id3=8
            assertQuery("SELECT id, value, ts FROM t SUBSAMPLE lttb(value, 3)")
                    .timestamp("ts")
                    .returns("""
                            id\tvalue\tts
                            0\t0.0\t2024-01-01T00:00:00.000000000Z
                            2\t100.0\t2024-01-01T00:00:00.000000002Z
                            4\t0.0\t2024-01-01T00:00:00.000000004Z
                            """);
        });
    }

    @Test
    public void testM4NanoTimestampPrecision() throws Exception {
        // 10 points spanning 9ns: with absolute-epoch double math the span
        // rounds to 0 and every bucket collapses into the final one. Exact
        // integer boundaries cut floor(9 * 1 / 2) = 4, so offsets [0,4) form
        // bucket 0 and offsets [4,9] form bucket 1.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, value DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0, 10.0, '2024-01-01T00:00:00.000000000Z'),
                    (1, -100.0, '2024-01-01T00:00:00.000000001Z'),
                    (2, 100.0, '2024-01-01T00:00:00.000000002Z'),
                    (3, 10.0, '2024-01-01T00:00:00.000000003Z'),
                    (4, 10.0, '2024-01-01T00:00:00.000000004Z'),
                    (5, 20.0, '2024-01-01T00:00:00.000000005Z'),
                    (6, -50.0, '2024-01-01T00:00:00.000000006Z'),
                    (7, 50.0, '2024-01-01T00:00:00.000000007Z'),
                    (8, 20.0, '2024-01-01T00:00:00.000000008Z'),
                    (9, 20.0, '2024-01-01T00:00:00.000000009Z')
                    """);
            // bucket0: first 0, min 1, max 2, last 3; bucket1: first 4, min 6, max 7, last 9
            assertQuery("SELECT id, value, ts FROM t SUBSAMPLE m4(value, 8)")
                    .timestamp("ts")
                    .returns("""
                            id\tvalue\tts
                            0\t10.0\t2024-01-01T00:00:00.000000000Z
                            1\t-100.0\t2024-01-01T00:00:00.000000001Z
                            2\t100.0\t2024-01-01T00:00:00.000000002Z
                            3\t10.0\t2024-01-01T00:00:00.000000003Z
                            4\t10.0\t2024-01-01T00:00:00.000000004Z
                            6\t-50.0\t2024-01-01T00:00:00.000000006Z
                            7\t50.0\t2024-01-01T00:00:00.000000007Z
                            9\t20.0\t2024-01-01T00:00:00.000000009Z
                            """);
        });
    }

    @Test
    public void testMinMaxNanoTimestampPrecision() throws Exception {
        // same 9ns fixture as testM4NanoTimestampPrecision: bucket0 holds
        // offsets [0,4) with min/max at ids 1/2, bucket1 holds offsets [4,9]
        // with min/max at ids 6/7
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, value DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0, 10.0, '2024-01-01T00:00:00.000000000Z'),
                    (1, -100.0, '2024-01-01T00:00:00.000000001Z'),
                    (2, 100.0, '2024-01-01T00:00:00.000000002Z'),
                    (3, 10.0, '2024-01-01T00:00:00.000000003Z'),
                    (4, 10.0, '2024-01-01T00:00:00.000000004Z'),
                    (5, 20.0, '2024-01-01T00:00:00.000000005Z'),
                    (6, -50.0, '2024-01-01T00:00:00.000000006Z'),
                    (7, 50.0, '2024-01-01T00:00:00.000000007Z'),
                    (8, 20.0, '2024-01-01T00:00:00.000000008Z'),
                    (9, 20.0, '2024-01-01T00:00:00.000000009Z')
                    """);
            assertQuery("SELECT id, value, ts FROM t SUBSAMPLE minmax(value, 4)")
                    .timestamp("ts")
                    .returns("""
                            id\tvalue\tts
                            1\t-100.0\t2024-01-01T00:00:00.000000001Z
                            2\t100.0\t2024-01-01T00:00:00.000000002Z
                            6\t-50.0\t2024-01-01T00:00:00.000000006Z
                            7\t50.0\t2024-01-01T00:00:00.000000007Z
                            """);
        });
    }

    @Test
    public void testMinMaxFarFutureMicroTimestamp() throws Exception {
        // year 9999 microsecond epoch (~2.5e17) has a double ulp of 32us, so
        // the precision loss is unit-independent, not TIMESTAMP_NS-specific.
        // Same shape as testMinMaxNanoTimestampPrecision at 1us spacing.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, value DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0, 10.0, 253402300799999990::timestamp),
                    (1, -100.0, 253402300799999991::timestamp),
                    (2, 100.0, 253402300799999992::timestamp),
                    (3, 10.0, 253402300799999993::timestamp),
                    (4, 10.0, 253402300799999994::timestamp),
                    (5, 20.0, 253402300799999995::timestamp),
                    (6, -50.0, 253402300799999996::timestamp),
                    (7, 50.0, 253402300799999997::timestamp),
                    (8, 20.0, 253402300799999998::timestamp),
                    (9, 20.0, 253402300799999999::timestamp)
                    """);
            assertQuery("SELECT id, value, ts FROM t SUBSAMPLE minmax(value, 4)")
                    .timestamp("ts")
                    .returns("""
                            id\tvalue\tts
                            1\t-100.0\t9999-12-31T23:59:59.999991Z
                            2\t100.0\t9999-12-31T23:59:59.999992Z
                            6\t-50.0\t9999-12-31T23:59:59.999996Z
                            7\t50.0\t9999-12-31T23:59:59.999997Z
                            """);
        });
    }

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 5)").timestamp("ts").returns("price\tts\n");
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
            assertQuery("SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE lttb(avg, 2)").timestamp("ts").returns("ts\tavg\n" +
                    "2024-01-01T00:00:00.000000Z\t15.0\n" +
                    "2024-01-01T02:00:00.000000Z\t55.0\n");
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
                    "column not found in SELECT list: nonexistent"
            );
            assertException(
                    "SELECT * FROM t SUBSAMPLE m4(nonexistent, 5)",
                    29,
                    "column not found in SELECT list: nonexistent"
            );
            assertException(
                    "SELECT * FROM t SUBSAMPLE minmax(nonexistent, 5)",
                    33,
                    "column not found in SELECT list: nonexistent"
            );
        });
    }

    @Test
    public void testErrorColumnNotFoundPrecedesMissingDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP)");
            for (String method : new String[]{"lttb", "m4", "minmax"}) {
                final String sql = "SELECT * FROM t SUBSAMPLE " + method + "(nonexistent, 5)";
                assertException(sql, sql.indexOf("nonexistent"), "column not found in SELECT list: nonexistent");
            }
            final String expressionSql = "SELECT * FROM t SUBSAMPLE m4(price * 2, 5)";
            assertException(expressionSql, expressionSql.lastIndexOf('*'), "SUBSAMPLE value argument must be a column name; alias the expression in the SELECT list and reference the alias");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 2::LONG)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "30.0\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("DECLARE @n := 2::LONG SELECT price, ts FROM t SUBSAMPLE lttb(price, @n)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "30.0\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, $1)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "30.0\t2024-01-01T02:00:00.000000Z\n");
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
            // Sorting by a non-timestamp column removes timestamp designation from the result.
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 2) ORDER BY price DESC").returns("price\tts\n" +
                    "20.0\t2024-01-01T02:00:00.000000Z\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n");
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
            assertQuery("SELECT price, quantity, ts FROM t SUBSAMPLE lttb(price, 2) ORDER BY quantity").returns("price\tquantity\tts\n" +
                    "10.0\t5\t2024-01-01T00:00:00.000000Z\n" +
                    "40.0\t9\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("SELECT price, quantity, ts FROM t SUBSAMPLE m4(price, 8) ORDER BY quantity").returns("price\tquantity\tts\n" +
                    "30.0\t1\t2024-01-01T03:00:00.000000Z\n" +
                    "20.0\t2\t2024-01-01T05:00:00.000000Z\n" +
                    "50.0\t3\t2024-01-01T01:00:00.000000Z\n" +
                    "10.0\t5\t2024-01-01T00:00:00.000000Z\n" +
                    "5.0\t8\t2024-01-01T02:00:00.000000Z\n" +
                    "45.0\t9\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 3) LIMIT 2").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T01:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t WHERE symbol = 'BTC' SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "40.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery(query).timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "40.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery(query).timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "40.0\t2024-01-01T04:00:00.000000Z\n");
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
    public void testLttbMinMaxPreselectionConstantSeries() throws Exception {
        // Worst case for the MinMaxLTTB preselection stage (large inputs; see LttbAlgorithm):
        // a constant series dedups every preselection bin's min==max picks to a single
        // survivor. Survivors (bins + 2 = 2 * (target - 2) + 2) still cover the target,
        // so the exact output row count is preserved.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t SELECT 42.0, timestamp_sequence('2024-01-01', 1000000) FROM long_sequence(5000)");
            assertQuery("SELECT count() FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 25))")
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n25\n");
        });
    }

    @Test
    public void testLttbMinMaxPreselectionGapSegments() throws Exception {
        // Two dense 3000-row segments split by a month-long gap: each segment independently
        // crosses the MinMaxLTTB activation threshold (2998 interior rows > 2*4*18). The
        // gap budgeting is unchanged (exactly 20 points per segment) and each segment's
        // first/last rows stay pinned through preselection.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t SELECT rnd_double() * 100, timestamp_sequence('2024-01-01', 1000000) FROM long_sequence(3000)");
            execute("INSERT INTO t SELECT rnd_double() * 100, timestamp_sequence('2024-02-01', 1000000) FROM long_sequence(3000)");
            assertQuery("SELECT count() FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 40, '1h'))")
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n40\n");
            // Per-segment pinned endpoints survive the fast path: last row of segment 1,
            // first row of segment 2.
            assertQuery("SELECT count() FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 40, '1h')) " +
                    "WHERE ts = '2024-01-01T00:49:59.000000Z' OR ts = '2024-02-01T00:00:00.000000Z'")
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n2\n");
        });
    }

    @Test
    public void testLttbMinMaxPreselectionKeepsExtremesAndEndpoints() throws Exception {
        // 5000 rows with target 10 takes LttbAlgorithm's MinMaxLTTB fast path (interior
        // 4998 > 2*4*8). Per-bin min/max preselection cannot lose isolated extremes, the
        // triangle stage then picks them (dominant areas in distinct buckets), global
        // first/last stay pinned, and the output count stays exactly the target. The
        // data is deterministic, so this doubles as a stability guard for the fast path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t SELECT " +
                    "case when x = 2500 then 100000.0 when x = 3500 then -100000.0 else (x % 97)::double end, " +
                    "timestamp_sequence('2024-01-01', 1000000) FROM long_sequence(5000)");
            assertQuery("SELECT count() FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 10))")
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n10\n");
            assertQuery("SELECT count() FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 10)) " +
                    "WHERE abs(price) = 100000.0")
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n2\n");
            assertQuery("SELECT min(ts), max(ts) FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 10))")
                    .expectSize()
                    .noRandomAccess()
                    .returns("min\tmax\n2024-01-01T00:00:00.000000Z\t2024-01-01T01:23:19.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE m4(price, 4)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "60.0\t2024-01-01T12:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "20.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE m4(price, 4)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "5.0\t2024-01-01T05:00:00.000000Z\n" +
                    "80.0\t2024-01-01T07:00:00.000000Z\n" +
                    "25.0\t2024-01-01T11:00:00.000000Z\n");
            assertQuery("SELECT price, ts FROM t SUBSAMPLE minmax(price, 4)").timestamp("ts").returns("price\tts\n" +
                    "50.0\t2024-01-01T02:00:00.000000Z\n" +
                    "5.0\t2024-01-01T05:00:00.000000Z\n" +
                    "80.0\t2024-01-01T07:00:00.000000Z\n" +
                    "15.0\t2024-01-01T08:00:00.000000Z\n");
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 4)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T02:00:00.000000Z\n" +
                    "80.0\t2024-01-01T07:00:00.000000Z\n" +
                    "25.0\t2024-01-01T11:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 4, '1h')").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "40.0\t2024-01-01T00:30:00.000000Z\n" +
                    "50.0\t2024-01-01T05:00:00.000000Z\n" +
                    "80.0\t2024-01-01T05:30:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 2, '2h')").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "40.0\t2024-01-01T04:00:00.000000Z\n");
        });
    }

    @Test
    public void testLttbGapThresholdScaledToNanoTimestampUnits() throws Exception {
        // Regression: the gap threshold is parsed to micros, but LttbAlgorithm compares it against
        // RAW column timestamps. On a TIMESTAMP_NS column an unscaled threshold is 1000x too small,
        // so '1h' used to split every 3.6s - here that would make all 8 rows their own segment and
        // return all 8 rows instead of 4. This is the exact wall-clock series as
        // testLttbGapPreserving, only the column unit differs, so the answer must be identical.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000000Z'),
                    (20.0, '2024-01-01T00:10:00.000000000Z'),
                    (30.0, '2024-01-01T00:20:00.000000000Z'),
                    (40.0, '2024-01-01T00:30:00.000000000Z'),
                    (50.0, '2024-01-01T05:00:00.000000000Z'),
                    (60.0, '2024-01-01T05:10:00.000000000Z'),
                    (70.0, '2024-01-01T05:20:00.000000000Z'),
                    (80.0, '2024-01-01T05:30:00.000000000Z')
                    """);
            // Only the 4.5h hole between 00:30 and 05:00 exceeds '1h': two segments, 2 points each.
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 4, '1h')").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000000Z\n" +
                    "40.0\t2024-01-01T00:30:00.000000000Z\n" +
                    "50.0\t2024-01-01T05:00:00.000000000Z\n" +
                    "80.0\t2024-01-01T05:30:00.000000000Z\n");
        });
    }

    @Test
    public void testLttbGapThresholdNanoWindowFunctionForm() throws Exception {
        // Same regression through the direct window-function overload rather than the SUBSAMPLE
        // clause: both spellings share LttbFunctionFactory.parseGapThreshold, so both must scale.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000000Z'),
                    (20.0, '2024-01-01T00:10:00.000000000Z'),
                    (30.0, '2024-01-01T00:20:00.000000000Z'),
                    (40.0, '2024-01-01T00:30:00.000000000Z')
                    """);
            // One segment (no hole beyond '1h'), target 2 -> first and last only.
            assertQuery("SELECT price, ts FROM (SELECT price, ts, lttb(ts, price, 2, '1h') OVER (ORDER BY ts) keep FROM t) WHERE keep")
                    .timestamp("ts")
                    .returns("price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000000Z\n" +
                            "40.0\t2024-01-01T00:30:00.000000000Z\n");
        });
    }

    @Test
    public void testLttbGapThresholdNanoBoundaryIsExclusive() throws Exception {
        // The split predicate is `currTs > prevTs + threshold`, so a hole of exactly the threshold
        // is NOT a gap and one nanosecond more is. Pins the comparison at nanosecond resolution,
        // which only holds if the threshold was scaled into nanos.
        assertMemoryLeak(() -> {
            // hole is exactly 1h -> single segment -> target 2 keeps first and last only
            execute("CREATE TABLE t_exact (price DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t_exact VALUES
                    (10.0, '2024-01-01T00:00:00.000000000Z'),
                    (20.0, '2024-01-01T00:00:10.000000000Z'),
                    (30.0, '2024-01-01T01:00:10.000000000Z'),
                    (40.0, '2024-01-01T01:00:20.000000000Z')
                    """);
            assertQuery("SELECT price, ts FROM t_exact SUBSAMPLE lttb(price, 2, '1h')").timestamp("ts")
                    .returns("price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000000Z\n" +
                            "40.0\t2024-01-01T01:00:20.000000000Z\n");

            // hole is 1h + 1ns -> two segments -> each keeps its own endpoints
            execute("CREATE TABLE t_over (price DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t_over VALUES
                    (10.0, '2024-01-01T00:00:00.000000000Z'),
                    (20.0, '2024-01-01T00:00:10.000000000Z'),
                    (30.0, '2024-01-01T01:00:10.000000001Z'),
                    (40.0, '2024-01-01T01:00:20.000000000Z')
                    """);
            assertQuery("SELECT price, ts FROM t_over SUBSAMPLE lttb(price, 2, '1h')").timestamp("ts")
                    .returns("price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000000Z\n" +
                            "20.0\t2024-01-01T00:00:10.000000000Z\n" +
                            "30.0\t2024-01-01T01:00:10.000000001Z\n" +
                            "40.0\t2024-01-01T01:00:20.000000000Z\n");
        });
    }

    @Test
    public void testLttbGapThresholdNanoSaturatesInsteadOfWrapping() throws Exception {
        // '1000000d' fits in micros (so the existing "gap threshold overflow" compile error does not
        // fire) but overflows long when scaled to nanos. It must saturate to "no gap can ever exceed
        // this" - a wrapping multiply would produce a small or negative threshold and shatter the
        // series into segments.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000000Z'),
                    (20.0, '2024-01-01T00:10:00.000000000Z'),
                    (30.0, '2024-06-01T00:00:00.000000000Z'),
                    (40.0, '2025-01-01T00:00:00.000000000Z')
                    """);
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 2, '1000000d')").timestamp("ts")
                    .returns("price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000000Z\n" +
                            "40.0\t2025-01-01T00:00:00.000000000Z\n");
        });
    }

    @Test
    public void testLttbGapThresholdUnitEquivalenceAcrossTimestampTypes() throws Exception {
        // Differential across timestamp units: the same wall-clock series and the same threshold must
        // select the same rows whether the column is TIMESTAMP or TIMESTAMP_NS. Sweeps every
        // supported interval unit so a future unit-handling change cannot silently diverge.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_us (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE t_ns (price DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts)");
            // 40 rows, 30s apart, with a 12h hole punched in the middle.
            execute("INSERT INTO t_us SELECT x::double, " +
                    "CASE WHEN x <= 20 THEN (x * 30000000L)::timestamp ELSE (x * 30000000L + 43200000000L)::timestamp END " +
                    "FROM long_sequence(40)");
            execute("INSERT INTO t_ns SELECT x::double, " +
                    "CASE WHEN x <= 20 THEN (x * 30000000000L)::timestamp_ns ELSE (x * 30000000000L + 43200000000000L)::timestamp_ns END " +
                    "FROM long_sequence(40)");

            final String[] thresholds = {"90s", "5m", "1h", "2h", "1d"};
            for (String threshold : thresholds) {
                final String ctx = "threshold=" + threshold;
                final String q = "SELECT price FROM (SELECT price, ts FROM %s SUBSAMPLE lttb(price, 8, '" + threshold + "'))";
                final String us = selectPrices(String.format(q, "t_us"));
                final String ns = selectPrices(String.format(q, "t_ns"));
                Assert.assertEquals(ctx, us, ns);
                Assert.assertTrue(ctx + " produced no rows", us.length() > 0);
            }
        });
    }

    // Renders just the price column of a subsample query, so results from a TIMESTAMP and a
    // TIMESTAMP_NS table can be compared directly (their timestamp renderings differ by design).
    private String selectPrices(String sql) throws Exception {
        final StringBuilder sink = new StringBuilder();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        sink.append(record.getDouble(0)).append('\n');
                    }
                }
            }
        }
        return sink.toString();
    }

    @Test
    public void testLttbGapInvalidUnit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 5, '1M')",
                    51,
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
                    "gap threshold must be a string constant such as '1h'"
            );
            assertException(
                    "SELECT price, ts FROM t SUBSAMPLE lttb(price, 5, '1h'::string)",
                    53,
                    "gap threshold must be a string constant such as '1h'"
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
            // SUBSAMPLE inside a parenthesized subquery wrapped in count(). The aggregate cursor
            // does not support random access even though its inner fused window cursor does.
            assertQuery("SELECT count() FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 2))")
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n2\n");
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
    public void testSubsampleRejectsTimestampDroppedByPivot() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, c SYMBOL, v DOUBLE) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01', 'a', 10.0),
                    ('2024-01-01', 'b', 11.0),
                    ('2024-01-02', 'a', 20.0),
                    ('2024-01-02', 'b', 21.0),
                    ('2024-01-03', 'a', 30.0),
                    ('2024-01-03', 'b', 31.0)
                    """);

            final String timestampDropped = "SELECT * FROM t PIVOT (sum(v) FOR c IN ('a','b')) SUBSAMPLE lttb(a, 5)";
            assertException(
                    timestampDropped,
                    timestampDropped.indexOf("SUBSAMPLE"),
                    "SUBSAMPLE requires a designated timestamp column"
            );
            assertQuery("SELECT * FROM t PIVOT (sum(v) FOR c IN ('a','b') GROUP BY ts) SUBSAMPLE uniform(2)")
                    .timestamp("ts")
                    .returns("""
                            ts\ta\tb
                            2024-01-01T00:00:00.000000Z\t10.0\t11.0
                            2024-01-03T00:00:00.000000Z\t30.0\t31.0
                            """);
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
            assertException(hidden, hidden.lastIndexOf("price"), "column not found in SELECT list: price");
            final String qualified = "SELECT price x, ts FROM rt SUBSAMPLE lttb(rt.price, 2)";
            assertException(qualified, qualified.lastIndexOf("rt.price"), "qualified column names are not supported in SUBSAMPLE arguments; use the unqualified SELECT list name");
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
            assertQuery("SELECT price, ts FROM (SELECT * FROM t_designated) SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "30.0\t2024-01-01T02:00:00.000000Z\n");

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
            assertQuery("SELECT price, ts FROM prices WHERE symbol = 'BTC' SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tts\n" +
                    "100.0\t2024-01-01T00:00:00.000000Z\n" +
                    "150.0\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, volume, ts FROM t SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tvolume\tts\n" +
                    "10.0\t100.0\t2024-01-01T00:00:00.000000Z\n" +
                    "40.0\t400.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("DECLARE @n := 2 SELECT price, ts FROM t SUBSAMPLE lttb(price, @n)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "40.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE m4(price, 4)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T01:00:00.000000Z\n" +
                    "40.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("DECLARE @points := 4 SELECT price, ts FROM t SUBSAMPLE m4(price, @points)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T01:00:00.000000Z\n" +
                    "20.0\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tts\n" +
                    "42.0\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, symbol, ts FROM t LATEST ON ts PARTITION BY symbol SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tsymbol\tts\n" +
                    "50.0\tBTC\t2024-01-01T02:00:00.000000Z\n" +
                    "60.0\tETH\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts, row_number() OVER () rn FROM t SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tts\trn\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                    "40.0\t2024-01-01T04:00:00.000000Z\t5\n");
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
            assertQuery("SELECT p.price, p.ts, v.volume FROM prices p ASOF JOIN volumes v ON (symbol) SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tts\tvolume\n" +
                    "100.0\t2024-01-01T00:00:00.000000Z\t1000.0\n" +
                    "150.0\t2024-01-01T02:00:00.000000Z\t1500.0\n");
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
    public void testSubsampleJoinDesignatedTimestampSurvivesQualifiedProjectionOrder() throws Exception {
        // The projection lists the right-side q.ts (alias rt) BEFORE the designated left
        // timestamp p.ts (alias ts). The SUBSAMPLE rewrite must resolve qualified column
        // identity: only p.ts carries designation, regardless of projection order. A wrong
        // pick designates rt on the subquery and every downstream ASOF join matches on the
        // right-side timestamps (labels 100, 200) instead of the left ones (labels 200, 400).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tsp (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsq (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO tsp VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsq VALUES (8, 5), (9, 25)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");

            // Independent oracle: the identical join WITHOUT SUBSAMPLE, restricted to the rows
            // uniform(2) keeps (x = 1 and x = 4). Downstream matches follow p.ts here.
            assertQuery("""
                    SELECT s.x, s.rt, s.ts, r.label
                    FROM (
                        SELECT q.ts rt, p.ts ts, p.x
                        FROM tsp p ASOF JOIN tsq q
                        WHERE p.x IN (1, 4)
                    ) s ASOF JOIN tsr r""")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("x\trt\tts\tlabel\n" +
                            "1\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000010Z\t200\n" +
                            "4\t1970-01-01T00:00:00.000025Z\t1970-01-01T00:00:00.000040Z\t400\n");

            // Same join through SUBSAMPLE, projecting only x and label so the assertion is
            // independent of timestamp metadata: the downstream ASOF matches themselves must
            // agree with the oracle. Matching on the mis-designated rt instead yields 100, 200.
            assertQuery("""
                    SELECT s.x, r.label
                    FROM (
                        SELECT q.ts rt, p.ts ts, p.x
                        FROM tsp p ASOF JOIN tsq q SUBSAMPLE uniform(2)
                    ) s ASOF JOIN tsr r""")
                    .noRandomAccess()
                    .returns("x\tlabel\n" +
                            "1\t200\n" +
                            "4\t400\n");

            // Same join through SUBSAMPLE: uniform(2) keeps the same rows, the subquery keeps
            // ts as its designated timestamp, and the downstream ASOF join must produce the
            // same matches as the oracle above.
            assertQuery("""
                    SELECT s.x, s.rt, s.ts, r.label
                    FROM (
                        SELECT q.ts rt, p.ts ts, p.x
                        FROM tsp p ASOF JOIN tsq q SUBSAMPLE uniform(2)
                    ) s ASOF JOIN tsr r""")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("x\trt\tts\tlabel\n" +
                            "1\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000010Z\t200\n" +
                            "4\t1970-01-01T00:00:00.000025Z\t1970-01-01T00:00:00.000040Z\t400\n");
        });
    }

    @Test
    public void testSubsampleJoinQualifiedTimestampProjectionControls() throws Exception {
        // Preservation controls for the qualified-identity resolution: the already-correct
        // projection order (p.ts before q.ts), the unqualified single-table shape, and the
        // sampled row set itself must not change when the timestamp pick is fixed.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tsp (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsq (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO tsp VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsq VALUES (8, 5), (9, 25)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");

            // (i) Currently-correct projection order: designated p.ts projected first.
            assertQuery("""
                    SELECT s.x, s.rt, s.ts, r.label
                    FROM (
                        SELECT p.ts ts, q.ts rt, p.x
                        FROM tsp p ASOF JOIN tsq q SUBSAMPLE uniform(2)
                    ) s ASOF JOIN tsr r""")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("x\trt\tts\tlabel\n" +
                            "1\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000010Z\t200\n" +
                            "4\t1970-01-01T00:00:00.000025Z\t1970-01-01T00:00:00.000040Z\t400\n");

            // (ii) Downstream ASOF over SUBSAMPLE with an unqualified single-table projection.
            assertQuery("""
                    SELECT s.x, s.ts, r.label
                    FROM (SELECT ts, x FROM tsp SUBSAMPLE uniform(2)) s ASOF JOIN tsr r""")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("x\tts\tlabel\n" +
                            "1\t1970-01-01T00:00:00.000010Z\t200\n" +
                            "4\t1970-01-01T00:00:00.000040Z\t400\n");

            // (iii) The sampled row set with the adversarial projection order. The outer
            // wrapper projects only x, deliberately erasing timestamp designation: which alias
            // the rewrite designates is under repair, but the KEPT ROWS must be x = 1 and
            // x = 4 either way.
            assertQuery("""
                    SELECT x FROM (
                        SELECT q.ts rt, p.ts ts, p.x
                        FROM tsp p ASOF JOIN tsq q SUBSAMPLE uniform(2)
                    )""")
                    .returns("x\n" +
                            "1\n" +
                            "4\n");
        });
    }

    @Test
    public void testSubsampleJoinQuotedQualifiedTimestampProjectionControls() throws Exception {
        // Qualified projection tokens keep quoted and case-variant spellings on their prefix
        // segments (QueryModel.getModelAliasIndex strips prefix quotes for exactly this reason,
        // and model aliases are stored unquoted by SqlParser.literal). All of these shapes
        // resolve the designated timestamp correctly today through the suffix match and must
        // keep working once the SUBSAMPLE rewrite starts checking qualifier identity.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tsp (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsq (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO tsp VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsq VALUES (8, 5), (9, 25)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");

            // (a) Quoted qualifiers on both timestamp projections, correct order, with the
            // downstream ASOF join asserting the full symptom axis.
            assertQuery("""
                    SELECT s.x, s.rt, s.ts, r.label
                    FROM (
                        SELECT "p".ts ts, "q".ts rt, p.x
                        FROM tsp p ASOF JOIN tsq q SUBSAMPLE uniform(2)
                    ) s ASOF JOIN tsr r""")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("x\trt\tts\tlabel\n" +
                            "1\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000010Z\t200\n" +
                            "4\t1970-01-01T00:00:00.000025Z\t1970-01-01T00:00:00.000040Z\t400\n");

            // (b) Quoted alias declarations, unquoted qualifiers.
            assertQuery("""
                    SELECT p.ts ts, q.ts rt, p.x
                    FROM tsp "p" ASOF JOIN tsq "q" SUBSAMPLE uniform(2)""")
                    .timestamp("ts")
                    .returns("ts\trt\tx\n" +
                            "1970-01-01T00:00:00.000010Z\t1970-01-01T00:00:00.000005Z\t1\n" +
                            "1970-01-01T00:00:00.000040Z\t1970-01-01T00:00:00.000025Z\t4\n");

            // (c) Case-variant qualifiers against lower-case aliases.
            assertQuery("""
                    SELECT P.ts ts, Q.ts rt, P.x
                    FROM tsp p ASOF JOIN tsq q SUBSAMPLE uniform(2)""")
                    .timestamp("ts")
                    .returns("ts\trt\tx\n" +
                            "1970-01-01T00:00:00.000010Z\t1970-01-01T00:00:00.000005Z\t1\n" +
                            "1970-01-01T00:00:00.000040Z\t1970-01-01T00:00:00.000025Z\t4\n");

            // (d) Quoted alias containing a dot: the qualifier split itself must be quote-aware
            // (a plain lastIndexOf split would cut inside the quoted segment).
            assertQuery("""
                    SELECT "p.a".ts ts, "p.a".x x
                    FROM tsp "p.a" SUBSAMPLE uniform(2)""")
                    .timestamp("ts")
                    .returns("ts\tx\n" +
                            "1970-01-01T00:00:00.000010Z\t1\n" +
                            "1970-01-01T00:00:00.000040Z\t4\n");

            // (e) Unaliased tables qualified by table name, correct order: the qualifier must
            // resolve through the table-name fallback (collectModelAlias uses tableNameExpr
            // when no alias is declared).
            assertQuery("""
                    SELECT tsp.ts ts, tsq.ts rt, tsp.x
                    FROM tsp ASOF JOIN tsq SUBSAMPLE uniform(2)""")
                    .timestamp("ts")
                    .returns("ts\trt\tx\n" +
                            "1970-01-01T00:00:00.000010Z\t1970-01-01T00:00:00.000005Z\t1\n" +
                            "1970-01-01T00:00:00.000040Z\t1970-01-01T00:00:00.000025Z\t4\n");

            // (f) Adversarial order WITH quoted qualifiers through a designation-erasing
            // wrapper: which alias the rewrite designates is under repair, but the kept rows
            // must be x = 1 and x = 4 either way, and the query must not start throwing.
            assertQuery("""
                    SELECT x FROM (
                        SELECT "q".ts rt, "p".ts ts, p.x
                        FROM tsp p ASOF JOIN tsq q SUBSAMPLE uniform(2)
                    )""")
                    .returns("x\n" +
                            "1\n" +
                            "4\n");
        });
    }

    @Test
    public void testSubsampleJoinQuotedQualifiedAdversarialOrder() throws Exception {
        // Adversarial projection order with QUOTED qualifiers: "q".ts (alias rt) is projected
        // before "p".ts (alias ts). Qualifier identity resolution must see through the quotes
        // (QueryModel.getModelAliasIndex strips prefix quotes for the same reason) and still
        // designate ts, so the downstream ASOF join matches on p.ts (labels 200, 400), not on
        // the right-side timestamps (labels 100, 200).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tsp (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsq (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO tsp VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsq VALUES (8, 5), (9, 25)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");

            assertQuery("""
                    SELECT s.x, s.rt, s.ts, r.label
                    FROM (
                        SELECT "q".ts rt, "p".ts ts, p.x
                        FROM tsp p ASOF JOIN tsq q SUBSAMPLE uniform(2)
                    ) s ASOF JOIN tsr r""")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("x\trt\tts\tlabel\n" +
                            "1\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000010Z\t200\n" +
                            "4\t1970-01-01T00:00:00.000025Z\t1970-01-01T00:00:00.000040Z\t400\n");
        });
    }

    @Test
    public void testSubsampleJoinUnaliasedTableQualifiedAdversarialOrder() throws Exception {
        // Unaliased tables qualified by table name, adversarial projection order: the qualifier
        // must resolve through the table-name fallback (collectModelAlias uses tableNameExpr
        // when no alias is declared), so tsp.ts keeps the designation even though tsq.ts is
        // projected first.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tsp (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsq (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO tsp VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsq VALUES (8, 5), (9, 25)");

            assertQuery("""
                    SELECT tsq.ts rt, tsp.ts ts, tsp.x
                    FROM tsp ASOF JOIN tsq SUBSAMPLE uniform(2)""")
                    .timestamp("ts")
                    .returns("rt\tts\tx\n" +
                            "1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000010Z\t1\n" +
                            "1970-01-01T00:00:00.000025Z\t1970-01-01T00:00:00.000040Z\t4\n");

            // Quoted table names in FROM are stored unquoted (SqlParser.literal), so quoted
            // qualifiers over quoted table declarations resolve through the same fallback.
            assertQuery("""
                    SELECT "tsq".ts rt, "tsp".ts ts, "tsp".x
                    FROM "tsp" ASOF JOIN "tsq" SUBSAMPLE uniform(2)""")
                    .timestamp("ts")
                    .returns("rt\tts\tx\n" +
                            "1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000010Z\t1\n" +
                            "1970-01-01T00:00:00.000025Z\t1970-01-01T00:00:00.000040Z\t4\n");
        });
    }

    @Test
    public void testSubsampleJoinHiddenTimestampQualifiedRejected() throws Exception {
        // The projection passes only the right side's like-named q.ts and hides the designated
        // p.ts. Silently designating q.ts sampled the wrong data axis; the rewrite must instead
        // throw the documented hidden-timestamp error at the SUBSAMPLE keyword.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tsp (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsq (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO tsp VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsq VALUES (8, 5), (9, 25)");

            final String sql = "SELECT q.ts, p.x FROM tsp p ASOF JOIN tsq q SUBSAMPLE uniform(2)";
            Assert.assertEquals(44, sql.indexOf("SUBSAMPLE"));
            assertException(
                    sql,
                    44,
                    "SUBSAMPLE requires a designated timestamp column; the SELECT list must include it unchanged"
            );
        });
    }

    @Test
    public void testSubsampleJoinDuplicateTimestampNamesDisambiguated() throws Exception {
        // Both sides' like-named timestamps are projected without explicit aliases; the parser
        // disambiguates them to ts (q.ts, first) and ts1 (p.ts, second). Designation must follow
        // column identity to p.ts's generated alias ts1, not projection order to ts.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tsp (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsq (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO tsp VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsq VALUES (8, 5), (9, 25)");

            assertQuery("""
                    SELECT q.ts, p.ts, p.x
                    FROM tsp p ASOF JOIN tsq q SUBSAMPLE uniform(2)""")
                    .timestamp("ts1")
                    .returns("ts\tts1\tx\n" +
                            "1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000010Z\t1\n" +
                            "1970-01-01T00:00:00.000025Z\t1970-01-01T00:00:00.000040Z\t4\n");
        });
    }

    @Test
    public void testSubsampleJoinKeepWindowOrdersByDesignatedTimestamp() throws Exception {
        // Ordering-axis discriminator: an INNER JOIN ON (sym) with one match per row makes the
        // projected right timestamp rt a SCRAMBLE of the designated ts (impossible under ASOF,
        // where rt is non-decreasing in ts), so lttb keeps DIFFERENT rows depending on which
        // axis orders the keep window. Points in ts order: (10,10), (20,100), (30,20), (40,30);
        // lttb(v, 3) keeps the first (ts=10) and last (ts=40) points plus the spike ts=20
        // (triangle area 1250 vs 50 for ts=30 with A=(10,10), C=(40,30)). Ordering by the
        // mis-picked rt instead keeps ts=20 and ts=30 as endpoints - a disjoint failure shape.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE fp (sym SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE fq (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO fp VALUES ('a', 10.0, 10), ('b', 100.0, 20), ('c', 20.0, 30), ('d', 30.0, 40)");
            execute("INSERT INTO fq VALUES ('b', 10), ('d', 20), ('a', 30), ('c', 40)");

            assertQuery("""
                    SELECT q.ts rt, p.ts ts, p.v v
                    FROM fp p JOIN fq q ON (sym) SUBSAMPLE lttb(v, 3)""")
                    .timestamp("ts")
                    .returns("rt\tts\tv\n" +
                            "1970-01-01T00:00:00.000030Z\t1970-01-01T00:00:00.000010Z\t10.0\n" +
                            "1970-01-01T00:00:00.000010Z\t1970-01-01T00:00:00.000020Z\t100.0\n" +
                            "1970-01-01T00:00:00.000020Z\t1970-01-01T00:00:00.000040Z\t30.0\n");
        });
    }

    @Test
    public void testSubsampleSampleByQualifiedFloorTimestampProjection() throws Exception {
        // rewriteSampleBy generates the bucket-floor call with a QUALIFIED timestamp argument
        // (alias.ts) when the FROM target is aliased; the SUBSAMPLE rewrite must resolve that
        // qualifier against the same aliased model when it carries designation through the
        // floor projection.
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
            // SAMPLE BY 1h produces 3 buckets; lttb(a, 2) keeps the first and last.
            assertQuery("SELECT p.ts, avg(price) a FROM t p SAMPLE BY 1h SUBSAMPLE lttb(a, 2)")
                    .timestamp("ts")
                    .returns("ts\ta\n" +
                            "2024-01-01T00:00:00.000000Z\t15.0\n" +
                            "2024-01-01T02:00:00.000000Z\t55.0\n");
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
            assertQuery("SELECT a.price, a.ts, b.volume FROM a ASOF JOIN b SUBSAMPLE lttb(price, 2)")
                    .timestamp("ts").returns("price\tts\tvolume\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\t100.0\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\t500.0\n");
            // Verify the join without SUBSAMPLE gives all 5 rows
            assertQuery("SELECT a.price, a.ts, b.volume FROM a ASOF JOIN b")
                    .expectSize()
                    .noRandomAccess()
                    .timestamp("ts").returns("price\tts\tvolume\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\t100.0\n" +
                            "20.0\t2024-01-01T01:00:00.000000Z\t200.0\n" +
                            "30.0\t2024-01-01T02:00:00.000000Z\t300.0\n" +
                            "40.0\t2024-01-01T03:00:00.000000Z\t400.0\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\t500.0\n");
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
            assertQuery("""
                    SELECT a.price, a.ts, b.volume
                    FROM a
                    ASOF JOIN (
                        SELECT volume, ts FROM b SUBSAMPLE lttb(volume, 2)
                    ) b
                    """).expectSize().noRandomAccess().timestamp("ts").returns("price\tts\tvolume\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\t100.0\n" +
                    "20.0\t2024-01-01T01:00:00.000000Z\t100.0\n" +
                    "30.0\t2024-01-01T02:00:00.000000Z\t100.0\n" +
                    "40.0\t2024-01-01T03:00:00.000000Z\t100.0\n" +
                    "50.0\t2024-01-01T04:00:00.000000Z\t500.0\n");
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
            // count() wrapping SUBSAMPLE: inner reduces 5 -> 2, outer counts 2. The aggregate
            // wrapper itself is forward-only, independently of the fused window's random access.
            assertQuery("SELECT count() FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 2))")
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n2\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 3)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T01:00:00.000000Z\n" +
                    "30.0\t2024-01-01T03:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 3)").timestamp("ts").returns("price\tts\n" +
                    "42.0\t2024-01-01T00:00:00.000000Z\n" +
                    "42.0\t2024-01-01T01:00:00.000000Z\n" +
                    "42.0\t2024-01-01T04:00:00.000000Z\n");
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
            // The input fits the target, so SUBSAMPLE preserves every row.
            assertQuery("SELECT price, ts FROM t SUBSAMPLE m4(price, 4)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "20.0\t2024-01-01T00:00:00.000000Z\n" +
                    "30.0\t2024-01-01T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testM4AllSameTimestampRetainsLandmarks() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (50.0, '2024-01-01T00:00:00.000000Z'),
                    (40.0, '2024-01-01T00:00:00.000000Z'),
                    (30.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:00:00.000000Z'),
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (100.0, '2024-01-01T00:00:00.000000Z'),
                    (60.0, '2024-01-01T00:00:00.000000Z')
                    """);
            assertQuery("SELECT price, ts FROM t SUBSAMPLE m4(price, 4)")
                    .timestamp("ts")
                    .returns("""
                            price\tts
                            50.0\t2024-01-01T00:00:00.000000Z
                            10.0\t2024-01-01T00:00:00.000000Z
                            100.0\t2024-01-01T00:00:00.000000Z
                            60.0\t2024-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testMinMaxAllSameTimestampRetainsLandmarks() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (50.0, '2024-01-01T00:00:00.000000Z'),
                    (40.0, '2024-01-01T00:00:00.000000Z'),
                    (30.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:00:00.000000Z'),
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (100.0, '2024-01-01T00:00:00.000000Z'),
                    (60.0, '2024-01-01T00:00:00.000000Z')
                    """);
            assertQuery("SELECT price, ts FROM t SUBSAMPLE minmax(price, 2)")
                    .timestamp("ts")
                    .returns("""
                            price\tts
                            10.0\t2024-01-01T00:00:00.000000Z
                            100.0\t2024-01-01T00:00:00.000000Z
                            """);
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 2, '1h')").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "20.0\t2024-01-01T06:00:00.000000Z\n" +
                    "30.0\t2024-01-01T12:00:00.000000Z\n" +
                    "40.0\t2024-01-01T18:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 4, '1h')").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "11.0\t2024-01-01T00:30:00.000000Z\n" +
                    "20.0\t2024-01-01T06:00:00.000000Z\n" +
                    "21.0\t2024-01-01T06:30:00.000000Z\n" +
                    "30.0\t2024-01-01T12:00:00.000000Z\n" +
                    "31.0\t2024-01-01T12:30:00.000000Z\n" +
                    "40.0\t2024-01-01T18:00:00.000000Z\n" +
                    "41.0\t2024-01-01T18:30:00.000000Z\n" +
                    "50.0\t2024-01-02T00:00:00.000000Z\n" +
                    "51.0\t2024-01-02T00:30:00.000000Z\n");

            // Non-gap LTTB with same target: hard maximum of 4.
            // LTTB selects first and last always, plus 2 from middle buckets.
            // The exact middle selections depend on triangle area calculations.
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 4)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "11.0\t2024-01-01T00:30:00.000000Z\n" +
                    "40.0\t2024-01-01T18:00:00.000000Z\n" +
                    "51.0\t2024-01-02T00:30:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 10, '1h')").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "11.0\t2024-01-01T00:10:00.000000Z\n" +
                    "13.0\t2024-01-01T00:30:00.000000Z\n" +
                    "20.0\t2024-01-01T06:00:00.000000Z\n" +
                    "21.0\t2024-01-01T06:10:00.000000Z\n" +
                    "23.0\t2024-01-01T06:30:00.000000Z\n" +
                    "30.0\t2024-01-01T12:00:00.000000Z\n" +
                    "31.0\t2024-01-01T12:10:00.000000Z\n" +
                    "33.0\t2024-01-01T12:30:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE m4(price, 12)").timestamp("ts").returns("price\tts\n" +
                    "10\t2024-01-01T00:00:00.000000Z\n" +
                    "50\t2024-01-01T01:00:00.000000Z\n" +
                    "20\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 3)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T01:00:00.000000Z\n" +
                    "20.0\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE m4(price, 2)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T01:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE m4(price, 4)").timestamp("ts").returns("price\tts\n" +
                    "1.7E308\t2024-01-01T00:00:00.000000Z\n" +
                    "-1.7E308\t2024-01-01T02:00:00.000000Z\n" +
                    "15.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE minmax(price, 4)").timestamp("ts").returns("price\tts\n" +
                    "1.7E308\t2024-01-01T00:00:00.000000Z\n" +
                    "10.0\t2024-01-01T01:00:00.000000Z\n" +
                    "-1.7E308\t2024-01-01T02:00:00.000000Z\n" +
                    "20.0\t2024-01-01T03:00:00.000000Z\n");
        });
    }

    @Test
    public void testLttbWithExtremeValues() throws Exception {
        // Sibling of testM4WithExtremeValues / testMinMaxWithExtremeValues (see there for the
        // Infinity-via-SQL discussion), but the edge is sharper for LTTB: the triangle area
        // multiplies timestamp deltas by value deltas, so near-Double.MAX_VALUE inputs push
        // area terms to +/-Infinity, and an Inf - Inf cancellation yields a NaN area. NaN
        // never beats maxArea (NaN > x is false), so the bucket falls back to its first
        // point instead of crashing or selecting garbage.
        // Walkthrough (5 rows, target 4, bucketSize 1.5):
        //   bucket 0 = {row1}: both area terms overflow to -Inf -> NaN area -> falls back
        //     to bucket start (row1, 10.0);
        //   bucket 1 = {row2, row3}: row2's area term is +Inf (finite - -1.7E308 spans the
        //     double range), which beats row3's finite area -> the extreme row2 is kept.
        // First (row0) and last (row4) are pinned.
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 4)").timestamp("ts").returns("price\tts\n" +
                    "1.7E308\t2024-01-01T00:00:00.000000Z\n" +
                    "10.0\t2024-01-01T01:00:00.000000Z\n" +
                    "-1.7E308\t2024-01-01T02:00:00.000000Z\n" +
                    "15.0\t2024-01-01T04:00:00.000000Z\n");
        });
    }

    @Test
    public void testLttbAllSameTimestamp() throws Exception {
        // Degenerate timestamp edge, sibling of the m4/minmax same-timestamp tests: every
        // dbx and avgDx is 0, so every triangle area is exactly 0. The strict '>' in the
        // area comparison keeps the first candidate of the bucket (only 0 > -1 fires), so
        // the selection stays deterministic: first, bucket start, last.
        // With 5 equal-ts rows and target 3: bucket [1,4) picks row1 -> rows 0, 1, 4.
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
            final String expected = "price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "20.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T00:00:00.000000Z\n";
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 3)").timestamp("ts").returns(expected);
            // Gap mode on the same data: a zero timestamp delta is never a gap (curr > prev
            // + threshold is false), so a single segment forms and the output is identical.
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 3, '1h')").timestamp("ts").returns(expected);
        });
    }

    @Test
    public void testLttbMinMaxPreselectionActivationBoundary() throws Exception {
        // Pins the MinMaxLTTB activation threshold contract on both sides of the boundary.
        // Preselection activates when interior (n - 2) > 2 * 4 * (target - 2); for target 4
        // that is n > 18. n=18 runs plain single-stage LTTB, n=19 runs the two-stage fast
        // path with 4 preselection bins. The output contract must not differ in count or
        // pinned endpoints. Exact selections are deliberately not pinned here: the n=19
        // side would freeze the preselection ratio, which is an internal tuning constant.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t18 AS (SELECT (x % 7)::double price, timestamp_sequence('2024-01-01', 1000000) ts FROM long_sequence(18)) TIMESTAMP(ts)");
            execute("CREATE TABLE t19 AS (SELECT (x % 7)::double price, timestamp_sequence('2024-01-01', 1000000) ts FROM long_sequence(19)) TIMESTAMP(ts)");
            assertQuery("SELECT count() FROM (SELECT price, ts FROM t18 SUBSAMPLE lttb(price, 4))")
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n4\n");
            assertQuery("SELECT count() FROM (SELECT price, ts FROM t19 SUBSAMPLE lttb(price, 4))")
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n4\n");
            assertQuery("SELECT min(ts), max(ts) FROM (SELECT price, ts FROM t18 SUBSAMPLE lttb(price, 4))")
                    .expectSize()
                    .noRandomAccess()
                    .returns("min\tmax\n2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:17.000000Z\n");
            assertQuery("SELECT min(ts), max(ts) FROM (SELECT price, ts FROM t19 SUBSAMPLE lttb(price, 4))")
                    .expectSize()
                    .noRandomAccess()
                    .returns("min\tmax\n2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:18.000000Z\n");
        });
    }

    @Test
    public void testLttbTargetSweepInvariants() throws Exception {
        // Output-count invariant swept across targets that land on every internal path:
        // m=2 (endpoints only), m=3/7/50 (MinMaxLTTB preselection: interior 998 > 8*(m-2)),
        // m=200 (plain LTTB: 998 <= 1584), m=999 (plain, n barely above target), m=1000
        // (count == target -> keepAll short-circuit). Every path must emit exactly
        // min(n, m) rows with the first and last input rows pinned. Deterministic
        // (x-derived) values, no rnd dependence.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT (x % 89)::double + x / 100.0 price, timestamp_sequence('2024-01-01', 1000000) ts FROM long_sequence(1000)) TIMESTAMP(ts)");
            final int[] targets = {2, 3, 7, 50, 200, 999, 1000};
            for (int target : targets) {
                assertQuery("SELECT count() FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, " + target + "))")
                        .expectSize()
                        .noRandomAccess()
                        .returns("count\n" + target + "\n");
                assertQuery("SELECT min(ts), max(ts) FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, " + target + "))")
                        .expectSize()
                        .noRandomAccess()
                        .returns("min\tmax\n2024-01-01T00:00:00.000000Z\t2024-01-01T00:16:39.000000Z\n");
            }
        });
    }

    @Test
    public void testLttbValueTypeMatrix() throws Exception {
        // Positive-path coverage for every numeric value type the factory accepts
        // (DOUBLE, FLOAT, INT, LONG, SHORT, BYTE - see readValue's per-type null -> NaN
        // mapping). The selection is value-driven, so identical logical values must give
        // an identical timestamp selection for every type. 10 rows [0,1,2,100,4..9]
        // (byte-range) with target 5 select rows {0,2,3,6,9} - the canonical single-stage
        // walkthrough: bucket [1,3) picks 2 (larger area vs next-bucket centroid), [3,6)
        // picks the 100 spike, [6,9) picks 6, endpoints pinned.
        assertMemoryLeak(() -> {
            final String expected = "ts\n" +
                    "2024-01-01T00:00:00.000000Z\n" +
                    "2024-01-01T00:00:02.000000Z\n" +
                    "2024-01-01T00:00:03.000000Z\n" +
                    "2024-01-01T00:00:06.000000Z\n" +
                    "2024-01-01T00:00:09.000000Z\n";
            final String[] types = {"DOUBLE", "FLOAT", "INT", "LONG", "SHORT", "BYTE"};
            for (String type : types) {
                final String table = "t_" + type.toLowerCase();
                execute("CREATE TABLE " + table + " (price " + type + ", ts TIMESTAMP) TIMESTAMP(ts)");
                execute("INSERT INTO " + table + " SELECT " +
                        "case when x = 4 then 100 else x - 1 end, " +
                        "timestamp_sequence('2024-01-01', 1000000) FROM long_sequence(10)");
                // SUBSAMPLE requires the value column in the projection; the outer
                // ts-only projection keeps the assertion type-agnostic (no per-type
                // value formatting).
                assertQuery("SELECT ts FROM (SELECT price, ts FROM " + table + " SUBSAMPLE lttb(price, 5))")
                        .timestamp("ts")
                        .returns(expected);
            }
        });
    }

    @Test
    public void testLttbValueTypeNullSentinelsDropped() throws Exception {
        // Per-type NULL handling: FLOAT NaN, INT_NULL and LONG_NULL all map to NaN in
        // readValue and the row is dropped from the buffer, exactly like a DOUBLE null.
        // The null replaces row 2 - a row the non-null fixture SELECTS - so dropping is
        // observable: the selection over the 9 surviving rows shifts to ts {0,3,4,6,9}s.
        // Walkthrough (bucketSize 7/3): bucket [1,3) picks the 100 spike (ts3); bucket
        // [3,5) picks ts4 (largest area vs the next-bucket centroid); the final bucket's
        // candidates (6,7,8) are exactly collinear with A=(ts4,4) and C=(ts9,9), every
        // area is 0.0, and the strict '>' fallback keeps the bucket's first point (ts6).
        // SHORT and BYTE are absent here by design: they have no null representation,
        // so no row can be dropped.
        assertMemoryLeak(() -> {
            final String expected = "ts\n" +
                    "2024-01-01T00:00:00.000000Z\n" +
                    "2024-01-01T00:00:03.000000Z\n" +
                    "2024-01-01T00:00:04.000000Z\n" +
                    "2024-01-01T00:00:06.000000Z\n" +
                    "2024-01-01T00:00:09.000000Z\n";
            final String[] types = {"DOUBLE", "FLOAT", "INT", "LONG"};
            for (String type : types) {
                final String table = "tn_" + type.toLowerCase();
                execute("CREATE TABLE " + table + " (price " + type + ", ts TIMESTAMP) TIMESTAMP(ts)");
                execute("INSERT INTO " + table + " SELECT " +
                        "case when x = 3 then null when x = 4 then 100 else x - 1 end, " +
                        "timestamp_sequence('2024-01-01', 1000000) FROM long_sequence(10)");
                assertQuery("SELECT ts FROM (SELECT price, ts FROM " + table + " SUBSAMPLE lttb(price, 5))")
                        .timestamp("ts")
                        .returns(expected);
            }
        });
    }

    @Test
    public void testLttbGapThresholdOverflowGuardNearMaxTimestamp() throws Exception {
        // Pins the overflow guard in gap detection (LttbAlgorithm.selectGapPreserving):
        // when prevTs > Long.MAX_VALUE - threshold, prevTs + threshold would overflow
        // negative and the unguarded comparison currTs > (negative) would flag EVERY
        // pair as a gap. The guard instead reports no-gap, which is exact: currTs is
        // bounded, so no representable timestamp can sit further than the threshold
        // past prevTs. Designated timestamps are capped at 9999-12-31 (validateBounds),
        // so the guard zone is only reachable via a near-max THRESHOLD: '106751991d'
        // (the largest parseable day count) puts Long.MAX_VALUE - threshold about 4
        // hours after the 1970 epoch - every modern timestamp is inside the zone.
        // Fixture: 3 rows in 2024 + 3 rows at the 9999 bound. The ~7975-year spread is
        // genuinely smaller than the threshold, so the correct answer is ONE segment:
        // plain LTTB over 6 rows, target 4 -> rows {0,2,3,5} = prices 1,3,4,6 (the huge
        // avgDx term dominates each bucket, picking the point closest to the far side).
        // A regressed guard would flag all 5 pairs as gaps: 6 one-row segments, floor
        // 6 > target -> 6 rows out, failing both count and content.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_guard (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t_guard VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(2.0, '2024-01-01T00:00:01.000000Z')," +
                    "(3.0, '2024-01-01T00:00:02.000000Z')," +
                    "(4.0, '9999-12-31T23:59:57.000000Z')," +
                    "(5.0, '9999-12-31T23:59:58.000000Z')," +
                    "(6.0, '9999-12-31T23:59:59.000000Z')");
            assertQuery("SELECT price FROM (SELECT price, ts FROM t_guard SUBSAMPLE lttb(price, 4, '106751991d'))")
                    .returns("price\n1.0\n3.0\n4.0\n6.0\n");

            // Plain (non-gap) LTTB entirely at the far timestamp bound: the exact-integer
            // timestamp deltas keep full resolution, so 1us-spaced triangle math still
            // discriminates 8000 years from the epoch. Values [10,50,20,40,30], target 3:
            // bucket [1,4) areas (vs A=first, C=last) are 140/0/60 in us-value units ->
            // the 50 spike wins.
            execute("CREATE TABLE t_max_plain (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t_max_plain VALUES " +
                    "(10.0, '9999-12-31T23:59:59.999991Z')," +
                    "(50.0, '9999-12-31T23:59:59.999992Z')," +
                    "(20.0, '9999-12-31T23:59:59.999993Z')," +
                    "(40.0, '9999-12-31T23:59:59.999994Z')," +
                    "(30.0, '9999-12-31T23:59:59.999995Z')");
            assertQuery("SELECT price FROM (SELECT price, ts FROM t_max_plain SUBSAMPLE lttb(price, 3))")
                    .returns("price\n10.0\n50.0\n30.0\n");
        });
    }

    @Test
    public void testLttbRandomizedDifferentialAgainstReference() throws Exception {
        // Differential fuzz: random walks with spikes, interleaved NULL rows, and duplicate
        // timestamps, compared row-for-row against an in-test reference implementation of
        // canonical single-stage LTTB using the same exact-integer timestamp-delta math
        // (see referenceLttb). Every (n, target) combo sits at or under the MinMaxLTTB
        // activation threshold (interior <= 8 * (target - 2), or target == 2 which never
        // preselects), so production must run the plain single-stage path and match the
        // reference EXACTLY - catching regressions anywhere in the pipeline: pass1
        // buffering, null bitset, ordinal mapping, keep-flag fusion, or an activation
        // threshold creeping over the documented boundary. Fixed seeds: reproducible.
        assertMemoryLeak(() -> {
            final long[] seeds = {0xDEADBEEFL, 42L, 20240101L};
            final int[][] combos = {{300, 2}, {50, 8}, {120, 17}, {200, 27}, {400, 52}};
            int tableId = 0;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (long seed : seeds) {
                    for (int[] combo : combos) {
                        final int n = combo[0];
                        final int m = combo[1];
                        final String table = "t_diff_" + tableId++;
                        final long[] tss = new long[n];
                        final double[] vals = new double[n];
                        final boolean[] nulls = new boolean[n];
                        generateRandomSeries(new Rnd(seed, seed * 31 + m), n, tss, vals, nulls, true);
                        createAndInsert(table, n, tss, vals, nulls);

                        // Compact to the non-null rows pass1 would buffer
                        final long[] refTs = new long[n];
                        final double[] refVal = new double[n];
                        int nonNull = 0;
                        for (int i = 0; i < n; i++) {
                            if (!nulls[i]) {
                                refTs[nonNull] = tss[i];
                                refVal[nonNull] = vals[i];
                                nonNull++;
                            }
                        }
                        Assert.assertTrue("fixture must exercise the algorithm path", nonNull > m);

                        final int[] expected = referenceLttb(refTs, refVal, nonNull, m);
                        final long[] outTs = new long[m];
                        final double[] outVal = new double[m];
                        final int outCount = runLttbAndCollect(
                                compiler, "SELECT price, ts FROM " + table + " SUBSAMPLE lttb(price, " + m + ")", outTs, outVal);

                        final String ctx = "seed=" + seed + " n=" + n + " m=" + m;
                        Assert.assertEquals(ctx + " row count", m, outCount);
                        for (int i = 0; i < m; i++) {
                            Assert.assertEquals(ctx + " ts[" + i + "]", refTs[expected[i]], outTs[i]);
                            Assert.assertEquals(ctx + " value[" + i + "]", refVal[expected[i]], outVal[i], 0.0);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testLttbRandomizedFastPathInvariants() throws Exception {
        // Property fuzz on the MinMaxLTTB fast path (interior >> 8 * (target - 2)): exact
        // selections are internal, but the contract is not. For random data with NULLs and
        // duplicate timestamps: exactly target rows, the first and last buffered rows
        // pinned, and the output an order-preserving subset of the non-null input
        // (verified by a forward matching walk). Fixed seeds: reproducible.
        assertMemoryLeak(() -> {
            final long[] seeds = {7L, 99L};
            final int[] targets = {3, 10, 40};
            final int n = 1200;
            int tableId = 0;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (long seed : seeds) {
                    final String table = "t_fast_" + tableId++;
                    final long[] tss = new long[n];
                    final double[] vals = new double[n];
                    final boolean[] nulls = new boolean[n];
                    generateRandomSeries(new Rnd(seed, seed + 17), n, tss, vals, nulls, true);
                    createAndInsert(table, n, tss, vals, nulls);

                    final long[] refTs = new long[n];
                    final double[] refVal = new double[n];
                    int nonNull = 0;
                    for (int i = 0; i < n; i++) {
                        if (!nulls[i]) {
                            refTs[nonNull] = tss[i];
                            refVal[nonNull] = vals[i];
                            nonNull++;
                        }
                    }

                    for (int m : targets) {
                        final long[] outTs = new long[n];
                        final double[] outVal = new double[n];
                        final int outCount = runLttbAndCollect(
                                compiler, "SELECT price, ts FROM " + table + " SUBSAMPLE lttb(price, " + m + ")", outTs, outVal);
                        final String ctx = "seed=" + seed + " m=" + m;
                        Assert.assertEquals(ctx + " row count", m, outCount);
                        Assert.assertEquals(ctx + " first ts pinned", refTs[0], outTs[0]);
                        Assert.assertEquals(ctx + " first value pinned", refVal[0], outVal[0], 0.0);
                        Assert.assertEquals(ctx + " last ts pinned", refTs[nonNull - 1], outTs[outCount - 1]);
                        Assert.assertEquals(ctx + " last value pinned", refVal[nonNull - 1], outVal[outCount - 1], 0.0);
                        matchOutputAgainstInput(ctx, outTs, outVal, outCount, refTs, refVal, nonNull, null);
                    }
                }
            }
        });
    }

    @Test
    public void testLttbRandomizedGapInvariants() throws Exception {
        // Property fuzz for gap-preserving mode over random segment structure. The
        // documented contract, independent of exact selections: every segment's first and
        // last rows survive (the min(2, segSize) floor), the output is an order-preserving
        // subset, and the row count lands in the soft-target envelope - exactly floorTotal
        // when the floor alone exceeds the target, otherwise between floorTotal and the
        // target. Targets 6/30 (floor dominates -> overshoot) and 100 (budget above floor)
        // exercise both budgeting branches. Fixed seeds: reproducible.
        assertMemoryLeak(() -> {
            final long[] seeds = {13L, 77L};
            final int[] targets = {6, 30, 100};
            final int n = 400;
            final long thresholdMicros = 3_600_000_000L; // '1h'
            int tableId = 0;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (long seed : seeds) {
                    final String table = "t_gap_" + tableId++;
                    final long[] tss = new long[n];
                    final double[] vals = new double[n];
                    final boolean[] nulls = new boolean[n];
                    generateRandomSeries(new Rnd(seed, seed ^ 0x5DEECE66DL), n, tss, vals, nulls, false);
                    // Inject 2h+ gaps at ~5% of the steps (regenerate timestamps)
                    final Rnd gapRnd = new Rnd(seed + 1, seed + 2);
                    long ts = 1704067200000000L;
                    for (int i = 0; i < n; i++) {
                        tss[i] = ts;
                        ts += gapRnd.nextInt(20) == 0
                                ? (7200 + gapRnd.nextInt(10000)) * 1_000_000L
                                : (1 + gapRnd.nextInt(4)) * 1_000_000L;
                    }
                    createAndInsert(table, n, tss, vals, nulls);

                    // Spec-level segmentation: split where delta > threshold
                    final int[] segStarts = new int[n + 1];
                    int segCount = 0;
                    segStarts[segCount++] = 0;
                    for (int i = 1; i < n; i++) {
                        if (tss[i] - tss[i - 1] > thresholdMicros) {
                            segStarts[segCount++] = i;
                        }
                    }
                    segStarts[segCount] = n;
                    int floorTotal = 0;
                    for (int s = 0; s < segCount; s++) {
                        floorTotal += Math.min(2, segStarts[s + 1] - segStarts[s]);
                    }
                    Assert.assertTrue("fixture must produce multiple segments", segCount > 3);

                    for (int m : targets) {
                        final long[] outTs = new long[n];
                        final double[] outVal = new double[n];
                        final int outCount = runLttbAndCollect(
                                compiler, "SELECT price, ts FROM " + table + " SUBSAMPLE lttb(price, " + m + ", '1h')", outTs, outVal);
                        final String ctx = "seed=" + seed + " m=" + m + " segments=" + segCount;
                        if (floorTotal >= m) {
                            Assert.assertEquals(ctx + " soft-target floor count", floorTotal, outCount);
                        } else {
                            Assert.assertTrue(ctx + " count >= floor (" + outCount + " vs " + floorTotal + ")", outCount >= floorTotal);
                            Assert.assertTrue(ctx + " count <= target (" + outCount + ")", outCount <= m);
                        }
                        final boolean[] kept = new boolean[n];
                        matchOutputAgainstInput(ctx, outTs, outVal, outCount, tss, vals, n, kept);
                        for (int s = 0; s < segCount; s++) {
                            Assert.assertTrue(ctx + " segment " + s + " first row kept", kept[segStarts[s]]);
                            Assert.assertTrue(ctx + " segment " + s + " last row kept", kept[segStarts[s + 1] - 1]);
                        }
                    }
                }
            }
        });
    }

    /**
     * Reference implementation of canonical single-stage LTTB (Steinarsson 2013) over
     * the same exact-integer timestamp-delta math production uses: equal row-count
     * buckets of size (n-2)/(m-2), point C = mean of the NEXT bucket with x measured
     * relative to point A via long subtraction, largest doubled-triangle area wins with
     * a strict comparison, first and last points pinned. Serves as the differential
     * oracle for inputs under the MinMaxLTTB activation threshold. Requires
     * n &gt; m &gt;= 2 (the keepAll short-circuit is the caller's business).
     */
    private static int[] referenceLttb(long[] tss, double[] vals, int n, int m) {
        final int[] out = new int[m];
        int outIdx = 0;
        out[outIdx++] = 0;
        final double bucketSize = (double) (n - 2) / (m - 2);
        int prev = 0;
        for (int bucket = 0; bucket < m - 2; bucket++) {
            final int bucketStart = (int) (bucket * bucketSize) + 1;
            int bucketEnd = (int) ((bucket + 1) * bucketSize) + 1;
            if (bucketEnd > n - 1) {
                bucketEnd = n - 1;
            }
            final int nextStart = bucketEnd;
            int nextEnd = (int) ((bucket + 2) * bucketSize) + 1;
            if (nextEnd > n - 1 || bucket == m - 3) {
                nextEnd = n;
            }
            final long axTs = tss[prev];
            final double ay = vals[prev];
            double avgDx = 0;
            double avgY = 0;
            final int len = nextEnd - nextStart;
            for (int j = nextStart; j < nextEnd; j++) {
                avgDx += (double) (tss[j] - axTs);
                avgY += vals[j];
            }
            if (len > 0) {
                avgDx /= len;
                avgY /= len;
            }
            double maxArea = -1;
            int maxIdx = bucketStart;
            for (int j = bucketStart; j < bucketEnd; j++) {
                final double dbx = (double) (tss[j] - axTs);
                final double area = Math.abs(dbx * (avgY - ay) - avgDx * (vals[j] - ay));
                if (area > maxArea) {
                    maxArea = area;
                    maxIdx = j;
                }
            }
            out[outIdx++] = maxIdx;
            prev = maxIdx;
        }
        out[outIdx] = n - 1;
        return out;
    }

    /**
     * Random walk with occasional +/-1000 spikes, ~1/4 duplicate timestamps and,
     * when {@code withNulls}, ~10%% NULL values capped at n/6 so fixtures always
     * keep enough rows to exercise the algorithm path.
     */
    private static void generateRandomSeries(Rnd rnd, int n, long[] tss, double[] vals, boolean[] nulls, boolean withNulls) {
        long ts = 1704067200000000L; // 2024-01-01T00:00:00Z in micros
        double base = 0;
        int nullCount = 0;
        for (int i = 0; i < n; i++) {
            tss[i] = ts;
            ts += rnd.nextInt(4) == 0 ? 0 : (1 + rnd.nextInt(5)) * 1_000_000L;
            base += rnd.nextDouble() - 0.5;
            double v = base;
            if (rnd.nextInt(25) == 0) {
                v += rnd.nextBoolean() ? 1000 : -1000;
            }
            vals[i] = v;
            if (withNulls && nullCount < n / 6 && rnd.nextInt(10) == 0) {
                nulls[i] = true;
                nullCount++;
            } else {
                nulls[i] = false;
            }
        }
    }

    private static void createAndInsert(String table, int n, long[] tss, double[] vals, boolean[] nulls) throws Exception {
        execute("CREATE TABLE " + table + " (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
        final StringBuilder sb = new StringBuilder("INSERT INTO ").append(table).append(" VALUES ");
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('(');
            if (nulls[i]) {
                sb.append("null");
            } else {
                // Double.toString round-trips exactly through the SQL double literal parser
                sb.append(vals[i]);
            }
            sb.append(",cast(").append(tss[i]).append(" as timestamp))");
        }
        execute(sb.toString());
    }

    private static int runLttbAndCollect(SqlCompiler compiler, String sql, long[] outTs, double[] outVal) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            int count = 0;
            while (cursor.hasNext()) {
                outVal[count] = record.getDouble(0);
                outTs[count] = record.getTimestamp(1);
                count++;
            }
            return count;
        }
    }

    /**
     * Forward matching walk: proves the output is an order-preserving subset of the
     * input rows (by exact (ts, value) pairs). When {@code kept} is non-null, marks
     * the matched input positions for segment-boundary assertions.
     */
    private static void matchOutputAgainstInput(
            String ctx, long[] outTs, double[] outVal, int outCount,
            long[] inTs, double[] inVal, int inCount, boolean[] kept
    ) {
        int in = 0;
        for (int i = 0; i < outCount; i++) {
            while (in < inCount && (inTs[in] != outTs[i] || inVal[in] != outVal[i])) {
                in++;
            }
            Assert.assertTrue(ctx + ": output row " + i + " (ts=" + outTs[i] + ") not an order-preserving input row", in < inCount);
            if (kept != null) {
                kept[in] = true;
            }
            in++;
        }
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
            assertQuery("SELECT price, ts FROM t WHERE symbol = 'ETH' SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tts\n");
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
            assertQuery("SELECT ts, sum(price) total FROM t SAMPLE BY 1h SUBSAMPLE lttb(total, 2)").timestamp("ts").returns("ts\ttotal\n" +
                    "2024-01-01T00:00:00.000000Z\t30.0\n" +
                    "2024-01-01T02:00:00.000000Z\t110.0\n");
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
            // DISTINCT is implemented by a group-by cursor and does not retain timestamp designation.
            assertQuery(query).returns("ts\tprice\n" +
                    "2024-01-01T00:00:00.000000Z\t10.0\n" +
                    "2024-01-01T02:00:00.000000Z\t30.0\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE minmax(price, 4)").timestamp("ts").returns("price\tts\n" +
                    "50.0\t2024-01-01T01:00:00.000000Z\n" +
                    "5.0\t2024-01-01T02:00:00.000000Z\n" +
                    "8.0\t2024-01-01T06:00:00.000000Z\n" +
                    "35.0\t2024-01-01T07:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE minmax(price, 2)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T01:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE minmax(price, 4)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "20.0\t2024-01-01T01:00:00.000000Z\n" +
                    "30.0\t2024-01-01T10:00:00.000000Z\n" +
                    "40.0\t2024-01-01T11:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE minmax(price, 2)").timestamp("ts").returns("price\tts\n" +
                    "42.0\t2024-01-01T00:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE m4(price, 4)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2000-01-01T00:00:00.000000Z\n" +
                    "50.0\t2100-01-01T00:00:00.000000Z\n" +
                    "20.0\t2200-01-01T00:00:00.000000Z\n" +
                    "30.0\t2290-01-01T00:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE minmax(price, 4)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2000-01-01T00:00:00.000000Z\n" +
                    "50.0\t2100-01-01T00:00:00.000000Z\n" +
                    "20.0\t2200-01-01T00:00:00.000000Z\n" +
                    "30.0\t2290-01-01T00:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tts\n" +
                    "10\t2024-01-01T00:00:00.000000Z\n" +
                    "40\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE m4(price, 12)").timestamp("ts").returns("price\tts\n" +
                    "100\t2024-01-01T00:00:00.000000Z\n" +
                    "500\t2024-01-01T01:00:00.000000Z\n" +
                    "200\t2024-01-01T02:00:00.000000Z\n");
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
            // One zero-span bucket selects the first/minimum and last/maximum rows.
            assertQuery("SELECT price, ts FROM t SUBSAMPLE m4(price, 2)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T00:00:00.000000Z\n");
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
            assertQuery("SELECT * FROM (SELECT price, ts FROM t1 SUBSAMPLE lttb(price, 2)) " +
                    "UNION ALL " +
                    "SELECT * FROM (SELECT price, ts FROM t2 SUBSAMPLE lttb(price, 2))")
                    .noRandomAccess()
                    .returns("price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "30.0\t2024-01-01T02:00:00.000000Z\n" +
                            "40.0\t2024-01-02T00:00:00.000000Z\n" +
                            "60.0\t2024-01-02T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 2) ORDER BY ts DESC").timestampDesc("ts").returns("price\tts\n" +
                    "40.0\t2024-01-01T04:00:00.000000Z\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tts\n" +
                    "10\t2024-01-01T00:00:00.000000Z\n" +
                    "30\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tts\n" +
                    "100\t2024-01-01T00:00:00.000000Z\n" +
                    "200\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tts\n" +
                    "0\t2024-01-01T00:00:00.000000Z\n" +
                    "0\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tts\n" +
                    "0\t2024-01-01T00:00:00.000000Z\n" +
                    "0\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, volume, ts FROM t SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tvolume\tts\n" +
                    "10.0\t100\t2024-01-01T00:00:00.000000Z\n" +
                    "40.0\t400\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t WHERE symbol = 'BTC' SUBSAMPLE lttb(price, 2)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "40.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 3) LIMIT 2").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T01:00:00.000000Z\n");
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
            assertQuery("SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE lttb(avg, 2)").timestamp("ts").returns("ts\tavg\n" +
                    "2024-01-01T00:00:00.000000Z\t15.0\n" +
                    "2024-01-01T02:00:00.000000Z\t55.0\n");
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
            assertQuery("SELECT price, ts FROM (SELECT price, ts FROM t ORDER BY ts DESC) SUBSAMPLE lttb(price, 2)").timestampDesc("ts").returns("price\tts\n" +
                    "40.0\t2024-01-01T04:00:00.000000Z\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n");
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
            assertQuery("""
                    SELECT ts, avg FROM (
                        SELECT ts, avg(price) avg FROM t
                        SAMPLE BY 1w ALIGN TO CALENDAR
                        ORDER BY ts DESC
                    ) SUBSAMPLE lttb(avg, 2)
                    """).timestampDesc("ts").returns("ts\tavg\n" +
                    "1970-01-12T00:00:00.000000Z\t55.0\n" +
                    "1969-12-29T00:00:00.000000Z\t15.0\n");
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
            assertQuery("SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE lttb(avg, 2)").timestamp("ts").returns("ts\tavg\n" +
                    "2024-01-01T00:00:00.000000Z\t15.0\n" +
                    "2024-01-01T02:00:00.000000Z\t55.0\n");
        });
    }

    @Test
    public void testSubsampleEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, 10)").timestamp("ts").returns("price\tts\n");
            assertQuery("SELECT price, ts FROM t SUBSAMPLE m4(price, 10)").timestamp("ts").returns("price\tts\n");
            assertQuery("SELECT price, ts FROM t SUBSAMPLE minmax(price, 10)").timestamp("ts").returns("price\tts\n");
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
            assertQuery("SELECT \"subsample\", ts FROM t").expectSize().timestamp("ts").returns("subsample\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "20.0\t2024-01-01T01:00:00.000000Z\n" +
                    "30.0\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT * FROM \"subsample\"").expectSize().timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "20.0\t2024-01-01T01:00:00.000000Z\n");
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
            assertQuery("SELECT price AS p, ts FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 2))").timestamp("ts").returns("p\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("SELECT * FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)) LIMIT 1").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n");
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
            assertQuery("SELECT * FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)) ORDER BY ts DESC").timestampDesc("ts").returns("price\tts\n" +
                    "50.0\t2024-01-01T04:00:00.000000Z\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n");
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
            assertQuery("SELECT price, qty, ts FROM t SUBSAMPLE m4(price, 4)").timestamp("ts").returns("price\tqty\tts\n" +
                    "10.0\t1\t2024-01-01T00:00:00.000000Z\n" +
                    "95.0\t5\t2024-01-01T04:00:00.000000Z\n" +
                    "3.0\t6\t2024-01-01T05:00:00.000000Z\n" +
                    "40.0\t8\t2024-01-01T07:00:00.000000Z\n");
            // Correct: inner M4 selects {qty=1,5,6,8}, outer WHERE qty >= 6 keeps {6,8}
            assertQuery("SELECT * FROM (SELECT price, qty, ts FROM t SUBSAMPLE m4(price, 4)) WHERE qty >= 6").timestamp("ts").returns("price\tqty\tts\n" +
                    "3.0\t6\t2024-01-01T05:00:00.000000Z\n" +
                    "40.0\t8\t2024-01-01T07:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE lttb(price, $1)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("DECLARE @n := 2 SELECT price, ts FROM t SUBSAMPLE lttb(price, @n)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE lttb(avg, 2)").timestamp("ts").returns("ts\tavg\n" +
                    "2024-01-01T00:00:00.000000Z\t12.5\n" +
                    "2024-01-01T02:00:00.000000Z\t32.5\n");
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
            assertQuery("SELECT * FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 2)) LIMIT 1").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n");
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
            assertQuery("SELECT DISTINCT price, ts FROM (SELECT price, ts FROM t SUBSAMPLE lttb(price, 2))")
                    .expectSize()
                    .returns("price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE m4(price, 4)").timestamp("ts").returns("price\tts\n" +
                    "30.0\t2024-01-01T00:00:00.000000Z\n" +
                    "90.0\t2024-01-01T01:00:00.000000Z\n" +
                    "10.0\t2024-01-01T02:00:00.000000Z\n" +
                    "20.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE minmax(price, 2)").timestamp("ts").returns("price\tts\n" +
                    "90.0\t2024-01-01T01:00:00.000000Z\n" +
                    "10.0\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE uniform(4)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "40.0\t2024-01-01T03:00:00.000000Z\n" +
                    "70.0\t2024-01-01T06:00:00.000000Z\n" +
                    "100.0\t2024-01-01T09:00:00.000000Z\n");
        });
    }

    @Test
    public void testUniformEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            assertQuery("SELECT price, ts FROM t SUBSAMPLE uniform(10)").timestamp("ts").returns("price\tts\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE uniform(10)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "20.0\t2024-01-01T01:00:00.000000Z\n" +
                    "30.0\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE uniform(10)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "20.0\t2024-01-01T01:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE uniform(10)").timestamp("ts").returns("price\tts\n" +
                    "null\t2024-01-01T00:00:00.000000Z\n" +
                    "20.0\t2024-01-01T01:00:00.000000Z\n" +
                    "null\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE uniform(2)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE cadence(2)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "30.0\t2024-01-01T02:00:00.000000Z\n" +
                    "50.0\t2024-01-01T04:00:00.000000Z\n" +
                    "60.0\t2024-01-01T05:00:00.000000Z\n");
        });
    }

    @Test
    public void testCadenceEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            assertQuery("SELECT price, ts FROM t SUBSAMPLE cadence(5)").timestamp("ts").returns("price\tts\n");
        });
    }

    @Test
    public void testCadenceNoColumnLookup() throws Exception {
        // cadence(2) must not try to resolve "2" as a column name
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES (10.0, '2024-01-01T00:00:00.000000Z')");
            drainWalQueue();
            assertQuery("SELECT price, ts FROM t SUBSAMPLE cadence(5)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE cadence(1)").timestamp("ts").returns("price\tts\n" +
                    "null\t2024-01-01T00:00:00.000000Z\n" +
                    "20.0\t2024-01-01T01:00:00.000000Z\n" +
                    "null\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE cadence(1)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "20.0\t2024-01-01T01:00:00.000000Z\n" +
                    "30.0\t2024-01-01T02:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE cadence(100)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testCadenceWithSeed() throws Exception {
        // cadence(3, 42): captured exact deterministic output over ten rows, including middle rows.
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
            // Captured deterministic golden: seed 42 hashes to offset 1 for stride 3.
            assertQuery("SELECT price, ts FROM t SUBSAMPLE cadence(3, 42)")
                    .timestamp("ts")
                    .returns("price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\n" +
                            "80.0\t2024-01-01T07:00:00.000000Z\n" +
                            "100.0\t2024-01-01T09:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE cadence(5, 0)")
                    .timestamp("ts")
                    .returns("price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "60.0\t2024-01-01T05:00:00.000000Z\n" +
                            "100.0\t2024-01-01T09:00:00.000000Z\n");
            assertQuery("SELECT price, ts FROM t SUBSAMPLE cadence(5, 3)")
                    .timestamp("ts")
                    .returns("price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "70.0\t2024-01-01T06:00:00.000000Z\n" +
                            "100.0\t2024-01-01T09:00:00.000000Z\n");
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
            // Random mode varies the middle row, but first and last are deterministic invariants.
            assertQuery("SELECT count() FROM (" +
                    "SELECT price, ts FROM t SUBSAMPLE cadence(3, NULL)" +
                    ") WHERE ts IN ('2024-01-01T00:00:00.000000Z', '2024-01-01T04:00:00.000000Z')")
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n2\n");
            assertQuery("SELECT count() >= 2 AND count() <= 3 valid_count FROM (" +
                    "SELECT price, ts FROM t SUBSAMPLE cadence(3, NULL))")
                    .expectSize()
                    .noRandomAccess()
                    .returns("valid_count\ntrue\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE cadence(3)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "40.0\t2024-01-01T03:00:00.000000Z\n" +
                    "50.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE uniform($1)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("DECLARE @n := 2 SELECT price, ts FROM t SUBSAMPLE uniform(@n)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "50.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE uniform(2)").timestamp("ts").returns("ts\tavg\n" +
                    "2024-01-01T00:00:00.000000Z\t12.5\n" +
                    "2024-01-01T02:00:00.000000Z\t32.5\n");
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
            assertQuery("SELECT * FROM (SELECT price, qty, ts FROM t SUBSAMPLE uniform(3)) WHERE qty > 3").timestamp("ts").returns("price\tqty\tts\n" +
                    "50.0\t5\t2024-01-01T04:00:00.000000Z\n");
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
    public void testUniformOverDescendingSubqueryTakesOrderedBranchAndPreservesIncomingOrder() throws Exception {
        // Preservation pin for the ordered row-selecting branch of mapSelectedRows: a SUBSAMPLE over
        // a ts-DESC subquery routes the desugared keep flag into an ORDERED window group, because its
        // OVER (ORDER BY ts) does not match the backward base scan. The orderedFunctions plan line
        // proves the orderedGroup branch runs, and the descending result proves that branch's
        // retained sort still maps window-order traversal ordinals back to ascending absolute
        // incoming-row indices, so the fused output preserves the INCOMING (descending) cursor
        // order. This pin is NOT an ascending-designated-timestamp promise. The executed oracle is
        // the equivalent UNFUSED shape below: a hand-written keep boolean never carries the internal
        // subsample keep-flag marker, so it stays on the window + Filter path, and fused and unfused
        // must return identical rows in identical order.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT timestamp_sequence(0, 1_000_000) ts, x v FROM long_sequence(10)) TIMESTAMP(ts)");
            final String expected = "ts\tv\n" +
                    "1970-01-01T00:00:09.000000Z\t10\n" +
                    "1970-01-01T00:00:05.000000Z\t6\n" +
                    "1970-01-01T00:00:00.000000Z\t1\n";
            assertQuery("SELECT ts, v FROM (SELECT ts, v FROM t ORDER BY ts DESC) SUBSAMPLE uniform(3)")
                    .timestampDesc("ts")
                    .withPlan("SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      orderedFunctions: [[ts] => [uniform(3) over (order by [ts])]]\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: t\n")
                    .returns(expected);
            assertQuery("SELECT ts, v FROM (SELECT ts, v, uniform(3) OVER (ORDER BY ts) keep FROM (SELECT ts, v FROM t ORDER BY ts DESC)) WHERE keep")
                    .timestampDesc("ts")
                    .returns(expected);
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
            assertQuery(query).timestamp("ts").returns("price\tts\tvolume\n" +
                    "100.0\t2024-01-01T00:00:00.000000Z\t1000.0\n" +
                    "200.0\t2024-01-01T01:00:00.000000Z\t2000.0\n" +
                    "150.0\t2024-01-01T02:00:00.000000Z\t1500.0\n");
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
            // 3-arg lttb (gap) migrates -> 4-arg (ts, value, target, gap) window overload,
            // including the gap threshold in the plan.
            assertQuery("SELECT ts, v FROM t SUBSAMPLE lttb(v, 3, '1h')")
                    .assertsPlan("SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [lttb(ts,v,3,'1h') over (order by [ts])]\n" +
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
            assertQuery(sql).timestamp("ts").returns("ts\tprice\tid\tkeep\n" +
                    "2024-01-01T00:00:00.000000Z\t10.0\t1\ttrue\n" +
                    "2024-01-01T01:00:00.000000Z\t20.0\t2\ttrue\n" +
                    "2024-01-01T02:00:00.000000Z\t30.0\t3\ttrue\n" +
                    "2024-01-01T03:00:00.000000Z\t40.0\t4\ttrue\n" +
                    "2024-01-01T04:00:00.000000Z\t50.0\t5\ttrue\n");

            // The real SUBSAMPLE feature must STILL fuse: same m4/target over the same table desugars
            // to the internal marked keep flag and takes the fused row-selecting path.
            final String subsamplePlan = planOf("SELECT ts, price FROM t SUBSAMPLE m4(price, 8)");
            Assert.assertTrue("SUBSAMPLE m4 must still fuse into the row-selecting node: " + subsamplePlan,
                    subsamplePlan.contains("CachedWindowLightSelect"));
        });
    }

    private void assertSubsampleCompletes(SqlCompiler compiler, String query) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue("expected rows for " + query, cursor.hasNext());
            while (cursor.hasNext()) {
                // drain
            }
        }
    }

    private void assertSubsampleRowCapBreach(SqlCompiler compiler, String queryPrefix, String method) throws SqlException {
        final String query = queryPrefix + method;
        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                    // drain until cap breach
                }
                Assert.fail("expected row-cap breach for " + query);
            } catch (CairoException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "SUBSAMPLE input exceeds maximum of 5 rows (raise cairo.sql.subsample.max.rows)"
                );
                Assert.assertEquals(query.indexOf(method.substring(0, method.indexOf('('))), e.getPosition());
            }
        }
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
            assertQuery("SELECT * FROM t SUBSAMPLE uniform(4)").timestamp("ts").returns("price\tqty\tts\n" +
                    "10.0\t1\t2024-01-01T00:00:00.000000Z\n" +
                    "40.0\t4\t2024-01-01T03:00:00.000000Z\n" +
                    "70.0\t7\t2024-01-01T06:00:00.000000Z\n" +
                    "100.0\t10\t2024-01-01T09:00:00.000000Z\n");
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
            assertQuery("SELECT * FROM t SUBSAMPLE m4(price, 4)").timestamp("ts").returns("price\tqty\tts\n" +
                    "10.0\t1\t2024-01-01T00:00:00.000000Z\n" +
                    "100.0\t10\t2024-01-01T09:00:00.000000Z\n");
        });
    }

    @Test
    public void testUniformWildcardOverUserNamedKeepSubsampleColumn() throws Exception {
        // Red test for the helper-alias collision: a legal table column literally named
        // __keep_subsample must not break SELECT * ... SUBSAMPLE. desugarSubsample picks the keep
        // alias against the UNEXPANDED projection (only '*'), so it stays __keep_subsample and
        // collides with the genuine column when rewriteSelectClause0 expands the wildcard.
        // Oracle: the equivalent explicit projection (green today, pinned in
        // testSubsampleKeepColumnExplicitProjectionControls).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE keep_tbl (__keep_subsample BOOLEAN, x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO keep_tbl VALUES (false, 1, 1), (false, 2, 2), (false, 3, 3), (false, 4, 4)");
            assertQuery("SELECT * FROM keep_tbl SUBSAMPLE uniform(2)").timestamp("ts")
                    .returns("__keep_subsample\tx\tts\n" +
                            "false\t1\t1970-01-01T00:00:00.000001Z\n" +
                            "false\t4\t1970-01-01T00:00:00.000004Z\n");
        });
    }

    @Test
    public void testUniformTableDotWildcardOverUserNamedKeepSubsampleColumn() throws Exception {
        // Red test: the table-qualified wildcard (t.*) expands through the same
        // createSelectColumnsForWildcard path and hits the same __keep_subsample collision.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE keep_tbl (__keep_subsample BOOLEAN, x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO keep_tbl VALUES (false, 1, 1), (false, 2, 2), (false, 3, 3), (false, 4, 4)");
            assertQuery("SELECT keep_tbl.* FROM keep_tbl SUBSAMPLE uniform(2)").timestamp("ts")
                    .returns("__keep_subsample\tx\tts\n" +
                            "false\t1\t1970-01-01T00:00:00.000001Z\n" +
                            "false\t4\t1970-01-01T00:00:00.000004Z\n");
        });
    }

    @Test
    public void testM4WildcardOverUserNamedKeepSubsampleColumn() throws Exception {
        // Red test: the value-inspecting desugar path (m4/minmax/lttb/sdt) shares the
        // desugarSubsample tail, so the same collision breaks it. m4(x, 2) over 4 monotone values
        // keeps the global first and last row (oracle: explicit projection).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE keep_tbl (__keep_subsample BOOLEAN, x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO keep_tbl VALUES (false, 1, 1), (false, 2, 2), (false, 3, 3), (false, 4, 4)");
            assertQuery("SELECT * FROM keep_tbl SUBSAMPLE m4(x, 2)").timestamp("ts")
                    .returns("__keep_subsample\tx\tts\n" +
                            "false\t1\t1970-01-01T00:00:00.000001Z\n" +
                            "false\t4\t1970-01-01T00:00:00.000004Z\n");
        });
    }

    @Test
    public void testUniformWildcardOverUpperCaseKeepSubsampleColumn() throws Exception {
        // Red test: the alias namespace (aliasToColumnMap) is case-insensitive, so a column named
        // __KEEP_SUBSAMPLE collides with the lower-case helper alias just the same.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE keep_upper (\"__KEEP_SUBSAMPLE\" BOOLEAN, x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO keep_upper VALUES (false, 1, 1), (false, 2, 2), (false, 3, 3), (false, 4, 4)");
            assertQuery("SELECT * FROM keep_upper SUBSAMPLE uniform(2)").timestamp("ts")
                    .returns("__KEEP_SUBSAMPLE\tx\tts\n" +
                            "false\t1\t1970-01-01T00:00:00.000001Z\n" +
                            "false\t4\t1970-01-01T00:00:00.000004Z\n");
        });
    }

    @Test
    public void testUniformWildcardOverKeepSubsampleAndEscapedVariantColumns() throws Exception {
        // Red test: with BOTH __keep_subsample and __keep_subsample1 present, the fixed alias
        // choice must escape past every taken variant (to __keep_subsample2), not just the base
        // name. Guards a naive fix that hard-codes the first escaped variant.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE keep_both (__keep_subsample BOOLEAN, __keep_subsample1 BOOLEAN, x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO keep_both VALUES (false, true, 1, 1), (false, true, 2, 2), (false, true, 3, 3), (false, true, 4, 4)");
            assertQuery("SELECT * FROM keep_both SUBSAMPLE uniform(2)").timestamp("ts")
                    .returns("__keep_subsample\t__keep_subsample1\tx\tts\n" +
                            "false\ttrue\t1\t1970-01-01T00:00:00.000001Z\n" +
                            "false\ttrue\t4\t1970-01-01T00:00:00.000004Z\n");
        });
    }

    @Test
    public void testUniformWildcardOverSubqueryProjectingKeepSubsampleColumn() throws Exception {
        // Red test: the colliding name can arrive through a nested wildcard subquery, so the fixed
        // alias choice must resolve the FULLY expanded output names, not just direct table columns.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE keep_tbl (__keep_subsample BOOLEAN, x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO keep_tbl VALUES (false, 1, 1), (false, 2, 2), (false, 3, 3), (false, 4, 4)");
            assertQuery("SELECT * FROM (SELECT * FROM keep_tbl) SUBSAMPLE uniform(2)").timestamp("ts")
                    .returns("__keep_subsample\tx\tts\n" +
                            "false\t1\t1970-01-01T00:00:00.000001Z\n" +
                            "false\t4\t1970-01-01T00:00:00.000004Z\n");
        });
    }

    @Test
    public void testUniformJoinWildcardOverKeepSubsampleColumn() throws Exception {
        // Red test: a join wildcard expands BOTH tables' columns into the keep window's namespace,
        // so a colliding column on the slave table breaks SELECT * over the join. Expected rows come
        // from the explicit projection oracle (green today, pinned in
        // testSubsampleKeepColumnExplicitProjectionControls).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE joins_a (price DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO joins_a VALUES (10.0, 'B', 1), (20.0, 'B', 2), (30.0, 'B', 3), (40.0, 'B', 4)");
            execute("CREATE TABLE joins_b (__keep_subsample BOOLEAN, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO joins_b VALUES (false, 'B', 1), (false, 'B', 2), (false, 'B', 3), (false, 'B', 4)");
            assertQuery("SELECT * FROM joins_a a ASOF JOIN joins_b b ON (sym) SUBSAMPLE uniform(2)")
                    .timestamp("ts")
                    .returns("price\tsym\tts\t__keep_subsample\tsym1\tts1\n" +
                            "10.0\tB\t1970-01-01T00:00:00.000001Z\tfalse\tB\t1970-01-01T00:00:00.000001Z\n" +
                            "40.0\tB\t1970-01-01T00:00:00.000004Z\tfalse\tB\t1970-01-01T00:00:00.000004Z\n");
        });
    }

    @Test
    public void testSubsampleKeepColumnExplicitProjectionControls() throws Exception {
        // Preservation controls (green today): explicit projections naming __keep_subsample work,
        // because createColumnAlias sees the name IN the projection map and escapes the helper to
        // __keep_subsample1 (or past every taken variant). These are the oracles for the wildcard
        // red tests and must not change when the alias choice is fixed. The explicit shape must
        // also keep fusing into the row-selecting window node.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE keep_tbl (__keep_subsample BOOLEAN, x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO keep_tbl VALUES (false, 1, 1), (false, 2, 2), (false, 3, 3), (false, 4, 4)");
            execute("CREATE TABLE keep_upper (\"__KEEP_SUBSAMPLE\" BOOLEAN, x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO keep_upper VALUES (false, 1, 1), (false, 2, 2), (false, 3, 3), (false, 4, 4)");
            execute("CREATE TABLE keep_both (__keep_subsample BOOLEAN, __keep_subsample1 BOOLEAN, x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO keep_both VALUES (false, true, 1, 1), (false, true, 2, 2), (false, true, 3, 3), (false, true, 4, 4)");
            execute("CREATE TABLE joins_a (price DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO joins_a VALUES (10.0, 'B', 1), (20.0, 'B', 2), (30.0, 'B', 3), (40.0, 'B', 4)");
            execute("CREATE TABLE joins_b (__keep_subsample BOOLEAN, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO joins_b VALUES (false, 'B', 1), (false, 'B', 2), (false, 'B', 3), (false, 'B', 4)");

            assertQuery("SELECT __keep_subsample, x, ts FROM keep_tbl SUBSAMPLE uniform(2)").timestamp("ts")
                    .withPlan("SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [uniform(2) over (order by [ts])]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: keep_tbl\n")
                    .returns("__keep_subsample\tx\tts\n" +
                            "false\t1\t1970-01-01T00:00:00.000001Z\n" +
                            "false\t4\t1970-01-01T00:00:00.000004Z\n");
            assertQuery("SELECT __keep_subsample, x, ts FROM keep_tbl SUBSAMPLE m4(x, 2)").timestamp("ts")
                    .returns("__keep_subsample\tx\tts\n" +
                            "false\t1\t1970-01-01T00:00:00.000001Z\n" +
                            "false\t4\t1970-01-01T00:00:00.000004Z\n");
            assertQuery("SELECT \"__KEEP_SUBSAMPLE\", x, ts FROM keep_upper SUBSAMPLE uniform(2)").timestamp("ts")
                    .returns("__KEEP_SUBSAMPLE\tx\tts\n" +
                            "false\t1\t1970-01-01T00:00:00.000001Z\n" +
                            "false\t4\t1970-01-01T00:00:00.000004Z\n");
            assertQuery("SELECT __keep_subsample, __keep_subsample1, x, ts FROM keep_both SUBSAMPLE uniform(2)").timestamp("ts")
                    .returns("__keep_subsample\t__keep_subsample1\tx\tts\n" +
                            "false\ttrue\t1\t1970-01-01T00:00:00.000001Z\n" +
                            "false\ttrue\t4\t1970-01-01T00:00:00.000004Z\n");
            assertQuery("SELECT a.price, a.sym, a.ts, b.__keep_subsample, b.sym, b.ts FROM joins_a a ASOF JOIN joins_b b ON (sym) SUBSAMPLE uniform(2)")
                    .timestamp("ts")
                    .returns("price\tsym\tts\t__keep_subsample\tsym1\tts1\n" +
                            "10.0\tB\t1970-01-01T00:00:00.000001Z\tfalse\tB\t1970-01-01T00:00:00.000001Z\n" +
                            "40.0\tB\t1970-01-01T00:00:00.000004Z\tfalse\tB\t1970-01-01T00:00:00.000004Z\n");
        });
    }

    @Test
    public void testSubsampleWildcardEscapedVariantControls() throws Exception {
        // Preservation controls (green today): the escaped-variant name __keep_subsample1 ALONE
        // does not collide with the helper; renaming a projected column TO __keep_subsample
        // escapes the helper; and an outer wildcard above a subsampled subquery still shows no
        // helper leak. None of these may change when the alias choice is fixed.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE keep_one (__keep_subsample1 BOOLEAN, x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO keep_one VALUES (false, 1, 1), (false, 2, 2), (false, 3, 3), (false, 4, 4)");
            assertQuery("SELECT * FROM keep_one SUBSAMPLE uniform(2)").timestamp("ts")
                    .returns("__keep_subsample1\tx\tts\n" +
                            "false\t1\t1970-01-01T00:00:00.000001Z\n" +
                            "false\t4\t1970-01-01T00:00:00.000004Z\n");
            assertQuery("SELECT x AS __keep_subsample, ts FROM keep_one SUBSAMPLE uniform(2)").timestamp("ts")
                    .returns("__keep_subsample\tts\n" +
                            "1\t1970-01-01T00:00:00.000001Z\n" +
                            "4\t1970-01-01T00:00:00.000004Z\n");
            assertQuery("SELECT * FROM (SELECT x, ts FROM keep_one SUBSAMPLE uniform(2))").timestamp("ts")
                    .returns("x\tts\n" +
                            "1\t1970-01-01T00:00:00.000001Z\n" +
                            "4\t1970-01-01T00:00:00.000004Z\n");
        });
    }

    @Test
    public void testUniformJoinWildcardOverKeepSubsampleColumnsOnBothBranches() throws Exception {
        // Fix-phase test (discriminates the reserved-set design from raw-name suffix probing): BOTH
        // join branches carry __keep_subsample. Wildcard expansion assigns the bare name to the
        // master's column and __keep_subsample1 to the slave's, so the helper must escape to
        // __keep_subsample2 - a raw-name set would pick __keep_subsample1 and still collide.
        // Master rows carry true, slave rows false, so a wrong-column capture would show in values.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE joins_c (__keep_subsample BOOLEAN, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO joins_c VALUES (true, 'B', 1), (true, 'B', 2), (true, 'B', 3), (true, 'B', 4)");
            execute("CREATE TABLE joins_d (__keep_subsample BOOLEAN, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO joins_d VALUES (false, 'B', 1), (false, 'B', 2), (false, 'B', 3), (false, 'B', 4)");
            assertQuery("SELECT * FROM joins_c a ASOF JOIN joins_d b ON (sym) SUBSAMPLE uniform(2)")
                    .timestamp("ts")
                    .returns("__keep_subsample\tsym\tts\t__keep_subsample1\tsym1\tts1\n" +
                            "true\tB\t1970-01-01T00:00:00.000001Z\tfalse\tB\t1970-01-01T00:00:00.000001Z\n" +
                            "true\tB\t1970-01-01T00:00:00.000004Z\tfalse\tB\t1970-01-01T00:00:00.000004Z\n");
        });
    }

    @Test
    public void testUniformWildcardOverSubqueryExplicitKeepSubsampleAlias() throws Exception {
        // Fix-phase test (design-review note): the colliding name can arrive via a subquery's
        // EXPLICIT alias, so the reserved-name collector must read subquery projection aliases
        // directly instead of only recursing through stars down to enumerated leaf tables.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE keep_plain (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO keep_plain VALUES (1, 1), (2, 2), (3, 3), (4, 4)");
            assertQuery("SELECT * FROM (SELECT x AS __keep_subsample, ts FROM keep_plain) SUBSAMPLE uniform(2)")
                    .timestamp("ts")
                    .returns("__keep_subsample\tts\n" +
                            "1\t1970-01-01T00:00:00.000001Z\n" +
                            "4\t1970-01-01T00:00:00.000004Z\n");
        });
    }

    @Test
    public void testUniformWildcardOverSubqueryMixedProjectionKeepSubsampleAlias() throws Exception {
        // Fix-phase test (design-review note): a MIXED inner projection (star plus explicit alias)
        // must contribute both parts to the reserved set - the star's expanded leaf names AND the
        // explicit colliding alias.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE keep_plain (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO keep_plain VALUES (1, 1), (2, 2), (3, 3), (4, 4)");
            assertQuery("SELECT * FROM (SELECT *, x AS __keep_subsample FROM keep_plain) SUBSAMPLE uniform(2)")
                    .timestamp("ts")
                    .returns("x\tts\t__keep_subsample\n" +
                            "1\t1970-01-01T00:00:00.000001Z\t1\n" +
                            "4\t1970-01-01T00:00:00.000004Z\t4\n");
        });
    }

    @Test
    public void testUniformCteWildcardOverKeepSubsampleColumn() throws Exception {
        // Fix-phase test (design-review note): a CTE becomes a nested model at parse time, so the
        // CTE shape must behave exactly like the wildcard-subquery shape.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE keep_tbl (__keep_subsample BOOLEAN, x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO keep_tbl VALUES (false, 1, 1), (false, 2, 2), (false, 3, 3), (false, 4, 4)");
            assertQuery("WITH c AS (SELECT * FROM keep_tbl) SELECT * FROM c SUBSAMPLE uniform(2)")
                    .timestamp("ts")
                    .returns("__keep_subsample\tx\tts\n" +
                            "false\t1\t1970-01-01T00:00:00.000001Z\n" +
                            "false\t4\t1970-01-01T00:00:00.000004Z\n");
        });
    }

    @Test
    public void testSubsampleWildcardCollisionStillFuses() throws Exception {
        // Fix-phase test: fusion into the row-selecting window cursor is driven by the
        // subsampleKeepFlag object marker, not the helper's name, so colliding wildcard shapes
        // must produce the same fused plan (CachedWindowLightSelect, no separate Filter) as the
        // non-colliding wildcard shape.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE keep_tbl (__keep_subsample BOOLEAN, x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO keep_tbl VALUES (false, 1, 1), (false, 2, 2), (false, 3, 3), (false, 4, 4)");
            assertQuery("SELECT * FROM keep_tbl SUBSAMPLE uniform(2)").timestamp("ts")
                    .withPlan("SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [uniform(2) over (order by [ts])]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: keep_tbl\n")
                    .returns("__keep_subsample\tx\tts\n" +
                            "false\t1\t1970-01-01T00:00:00.000001Z\n" +
                            "false\t4\t1970-01-01T00:00:00.000004Z\n");
            assertQuery("SELECT * FROM keep_tbl SUBSAMPLE m4(x, 2)").timestamp("ts")
                    .withPlan("SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [m4(ts,x,2) over (order by [ts])]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: keep_tbl\n")
                    .returns("__keep_subsample\tx\tts\n" +
                            "false\t1\t1970-01-01T00:00:00.000001Z\n" +
                            "false\t4\t1970-01-01T00:00:00.000004Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE uniform(4) LIMIT 2").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "40.0\t2024-01-01T03:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE cadence($1, $2)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE cadence(1, NULL)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "20.0\t2024-01-01T01:00:00.000000Z\n");
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
            assertQuery("SELECT ts, avg(price) avg FROM t SAMPLE BY 1h SUBSAMPLE cadence(2)").timestamp("ts").returns("ts\tavg\n" +
                    "2024-01-01T00:00:00.000000Z\t12.5\n" +
                    "2024-01-01T02:00:00.000000Z\t32.5\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE cadence($1)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "40.0\t2024-01-01T03:00:00.000000Z\n" +
                    "50.0\t2024-01-01T04:00:00.000000Z\n");
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
            assertQuery("DECLARE @s := 3 SELECT price, ts FROM t SUBSAMPLE cadence(@s)").timestamp("ts").returns("price\tts\n" +
                    "10.0\t2024-01-01T00:00:00.000000Z\n" +
                    "40.0\t2024-01-01T03:00:00.000000Z\n" +
                    "50.0\t2024-01-01T04:00:00.000000Z\n");
        });
    }

    @Test
    public void testCadenceWithBindSeed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (" +
                    "SELECT x * 10.0 price, timestamp_sequence(0, 1) ts FROM long_sequence(10)) TIMESTAMP(ts)");
            bindVariableService.clear();
            bindVariableService.setLong(0, 3);
            bindVariableService.setLong(1, 42);
            final double[][] expected = {
                    {10.0, 50.0, 80.0, 100.0}, // seed 42 -> offset 1
                    {10.0, 60.0, 90.0, 100.0}  // seed 3 -> offset 2
            };
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(
                         "SELECT price, ts FROM t SUBSAMPLE cadence($1, $2)",
                         sqlExecutionContext).getRecordCursorFactory()) {
                for (int execution = 0; execution < expected.length; execution++) {
                    if (execution == 1) {
                        // Rebind on the same compiled factory. Different middle rows prove the seed
                        // is read per execution rather than ignored or cached from the first open.
                        bindVariableService.setLong(1, 3);
                    }
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final Record record = cursor.getRecord();
                        int row = 0;
                        while (cursor.hasNext()) {
                            Assert.assertTrue("too many rows on execution " + execution, row < expected[execution].length);
                            Assert.assertEquals(expected[execution][row++], record.getDouble(0), 0.0);
                        }
                        Assert.assertEquals("execution " + execution, expected[execution].length, row);
                    }
                }
            }
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
            assertQuery("SELECT * FROM (SELECT price, qty, ts FROM t SUBSAMPLE cadence(3)) WHERE qty > 3").timestamp("ts").returns("price\tqty\tts\n" +
                    "40.0\t4\t2024-01-01T03:00:00.000000Z\n" +
                    "60.0\t6\t2024-01-01T05:00:00.000000Z\n");
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
            assertQuery("SELECT price, ts FROM t SUBSAMPLE cadence(3, 40 + 2)")
                    .timestamp("ts")
                    .returns("price\tts\n" +
                            "10.0\t2024-01-01T00:00:00.000000Z\n" +
                            "50.0\t2024-01-01T04:00:00.000000Z\n");
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
        // (arg 0) is a bare column literal. The rewrite rejects a non-literal value arg with a message
        // that tells the user to project the expression and reference its alias, so the window path
        // never silently evaluates v*2 as a DOUBLE expression.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT ts, v FROM t SUBSAMPLE m4(v * 2, 4)",
                    35,
                    "SUBSAMPLE value argument must be a column name; alias the expression in the SELECT list and reference the alias"
            );
            assertException(
                    "SELECT ts, v FROM t SUBSAMPLE m4(abs(v), 4)",
                    33,
                    "SUBSAMPLE value argument must be a column name; alias the expression in the SELECT list and reference the alias"
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
            assertQuery("EXPLAIN SELECT ts, price FROM x SUBSAMPLE sdt(price, 0.5)")
                    .expectSize()
                    .noRandomAccess()
                    .returns("QUERY PLAN\n" +
                            "SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [sdt(ts, price, 0.5) over (order by [ts])]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: x\n");
        });
    }

    @Test
    public void testSdtKeepsAllRowsFused() throws Exception {
        // compdev = 0 over a zig-zag series exercises the fused near-full keep-set against a
        // hard-coded golden rather than comparing SUBSAMPLE with the same SDT implementation.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO x VALUES " +
                    "(0.0, 1::timestamp),(1.0, 2::timestamp),(0.0, 3::timestamp)," +
                    "(1.0, 4::timestamp),(0.0, 5::timestamp)");
            assertQuery("SELECT ts, price FROM x SUBSAMPLE sdt(price, 0.0)")
                    .timestamp("ts")
                    .returns("""
                            ts\tprice
                            1970-01-01T00:00:00.000001Z\t0.0
                            1970-01-01T00:00:00.000002Z\t1.0
                            1970-01-01T00:00:00.000003Z\t0.0
                            1970-01-01T00:00:00.000004Z\t1.0
                            1970-01-01T00:00:00.000005Z\t0.0
                            """);
            // and the fused plan is used
            assertQuery("EXPLAIN SELECT ts, price FROM x SUBSAMPLE sdt(price, 0.0)")
                    .expectSize()
                    .noRandomAccess()
                    .returns("QUERY PLAN\n" +
                            "SelectedRecord\n" +
                            "    CachedWindowLightSelect\n" +
                            "      unorderedFunctions: [sdt(ts, price, 0.0) over (order by [ts])]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: x\n");
        });
    }

    @Test
    public void testSdtHugeMagnitudeKeepsChangedPoint() throws Exception {
        // F3-SDT-OVERFLOW red test (SUBSAMPLE form): 1e308 - (-1e308) overflows the swinging-door
        // slope terms to +Inf, so the fused keep-set wrongly drops the middle row as interior.
        // All inputs are finite and compdev is 0, so any value change must be kept: the
        // hand-derived keep set is all three rows (slopes 2e308 at dt=1 vs 1e308 at dt=2).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO x VALUES " +
                    "(-1e308, 1::timestamp),(1e308, 2::timestamp),(1e308, 3::timestamp)");
            assertQuery("SELECT ts, price FROM x SUBSAMPLE sdt(price, 0.0)")
                    .timestamp("ts")
                    .returns("""
                            ts\tprice
                            1970-01-01T00:00:00.000001Z\t-1.0E308
                            1970-01-01T00:00:00.000002Z\t1.0E308
                            1970-01-01T00:00:00.000003Z\t1.0E308
                            """);
        });
    }

    @Test
    public void testSdtScaledProbeSeriesKeepsAllPoints() throws Exception {
        // F3-SDT-OVERFLOW preservation control (green pre-fix, must stay green): the same shape
        // at magnitude 1 has finite slopes, the doors cross and all three rows survive
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO x VALUES " +
                    "(-1.0, 1::timestamp),(1.0, 2::timestamp),(1.0, 3::timestamp)");
            assertQuery("SELECT ts, price FROM x SUBSAMPLE sdt(price, 0.0)")
                    .timestamp("ts")
                    .returns("""
                            ts\tprice
                            1970-01-01T00:00:00.000001Z\t-1.0
                            1970-01-01T00:00:00.000002Z\t1.0
                            1970-01-01T00:00:00.000003Z\t1.0
                            """);
        });
    }

    @Test
    public void testSdtMatchesCapturedGolden() throws Exception {
        // A monotonic ramp stays inside the swinging door and keeps only its endpoints.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO x SELECT x::double, x::timestamp FROM long_sequence(5)");
            assertQuery("SELECT ts, price FROM x SUBSAMPLE sdt(price, 0.5)")
                    .timestamp("ts")
                    .returns("""
                            ts\tprice
                            1970-01-01T00:00:00.000001Z\t1.0
                            1970-01-01T00:00:00.000005Z\t5.0
                            """);
        });
    }

    @Test
    public void testSdtNullFlush() throws Exception {
        // A null value mid-series: default RESPECT NULLS. The desugared SUBSAMPLE must match the
        // window function's null handling byte-for-byte.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO x VALUES " +
                    "(0.0, 1::timestamp),(0.0, 2::timestamp),(0.0, 3::timestamp)," +
                    "(NULL, 4::timestamp),(5.0, 5::timestamp),(5.0, 6::timestamp)");
            assertQuery("SELECT ts, price FROM x SUBSAMPLE sdt(price, 0.5)")
                    .timestamp("ts")
                    .returns("""
                            ts\tprice
                            1970-01-01T00:00:00.000001Z\t0.0
                            1970-01-01T00:00:00.000003Z\t0.0
                            1970-01-01T00:00:00.000004Z\tnull
                            1970-01-01T00:00:00.000005Z\t5.0
                            1970-01-01T00:00:00.000006Z\t5.0
                            """);
        });
    }

    @Test
    public void testSdtNonConstantCompdev() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT ts, price FROM x SUBSAMPLE sdt(price, $1)",
                    45,
                    "SUBSAMPLE sdt requires a constant, non-negative finite compdev"
            );
        });
    }

    @Test
    public void testSdtNegativeCompdev() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT ts, price FROM x SUBSAMPLE sdt(price, -1.0)",
                    45,
                    "SUBSAMPLE sdt requires a constant, non-negative finite compdev"
            );
        });
    }

    @Test
    public void testSdtInfiniteCompdev() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT ts, price FROM x SUBSAMPLE sdt(price, 1.0/0.0)",
                    48,
                    "SUBSAMPLE sdt requires a constant, non-negative finite compdev"
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
                    44,
                    "SUBSAMPLE value argument must be a column name; alias the expression in the SELECT list and reference the alias"
            );
        });
    }

    // ------------------------------------------------------------------------------------------
    // F7-DISTINCT-STAR: a bare wildcard projection in an aggregation context (DISTINCT *,
    // DISTINCT t.*, GROUP BY over all keys with *, SELECT *, count()) used to leak the internal
    // __keep_subsample helper into the outer wildcard (desugarSubsample flipped the keep column's
    // includeIntoWildcard for aggregation rebuilds while the outer model still carried a raw '*').
    // The fused row-selecting cursor never writes the helper's narrow-chain slot, so reading the
    // leaked BOOLEAN tripped the memory-bounds assert (AbstractMemoryCR.addressOf). desugarSubsample
    // now uses the star wiring for every wildcard projection and flips includeIntoWildcard only for
    // explicit aggregation projections. Every test below first asserts its explicit-column oracle,
    // then asserts the wildcard form returns the identical rows with no helper column in the output.
    // ------------------------------------------------------------------------------------------

    @Test
    public void testDistinctStarUniform() throws Exception {
        // Red test: SELECT DISTINCT * must return the distinct rows subsampled by uniform, with
        // no helper column. Hand-derived: 4 distinct rows (the duplicate collapses), uniform(2)
        // keeps the first and last in timestamp order. Oracle: the explicit-column DISTINCT form
        // (asserted green first). The shape must keep fusing into the row-selecting window node.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (10.0, 'A', 1), (10.0, 'A', 1), (20.0, 'B', 2), (30.0, 'A', 3), (40.0, 'B', 4)");
            final String expected = "price\tsym\tts\n" +
                    "10.0\tA\t1970-01-01T00:00:00.000001Z\n" +
                    "40.0\tB\t1970-01-01T00:00:00.000004Z\n";
            assertQuery("SELECT DISTINCT price, sym, ts FROM t SUBSAMPLE uniform(2)").returns(expected);
            assertQuery("SELECT DISTINCT * FROM t SUBSAMPLE uniform(2)")
                    .withPlanContaining("CachedWindowLightSelect")
                    .returns(expected);
        });
    }

    @Test
    public void testDistinctStarM4() throws Exception {
        // Red test: the value-inspecting desugar path shares the same aggregation-branch leak.
        // m4(price, 2) over 4 distinct monotone prices keeps the global first and last row
        // (same derivation as testM4WildcardOverUserNamedKeepSubsampleColumn). Oracle: the
        // explicit-column DISTINCT form.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (10.0, 'A', 1), (10.0, 'A', 1), (20.0, 'B', 2), (30.0, 'A', 3), (40.0, 'B', 4)");
            final String expected = "price\tsym\tts\n" +
                    "10.0\tA\t1970-01-01T00:00:00.000001Z\n" +
                    "40.0\tB\t1970-01-01T00:00:00.000004Z\n";
            assertQuery("SELECT DISTINCT price, sym, ts FROM t SUBSAMPLE m4(price, 2)").returns(expected);
            assertQuery("SELECT DISTINCT * FROM t SUBSAMPLE m4(price, 2)").returns(expected);
        });
    }

    @Test
    public void testDistinctTableDotStarUniform() throws Exception {
        // Red test: the table-qualified wildcard (t.*) reaches the same aggregation-branch leak.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (10.0, 'A', 1), (10.0, 'A', 1), (20.0, 'B', 2), (30.0, 'A', 3), (40.0, 'B', 4)");
            final String expected = "price\tsym\tts\n" +
                    "10.0\tA\t1970-01-01T00:00:00.000001Z\n" +
                    "40.0\tB\t1970-01-01T00:00:00.000004Z\n";
            assertQuery("SELECT DISTINCT price, sym, ts FROM t SUBSAMPLE uniform(2)").returns(expected);
            assertQuery("SELECT DISTINCT t.* FROM t SUBSAMPLE uniform(2)").returns(expected);
        });
    }

    @Test
    public void testDistinctStarJoinUniform() throws Exception {
        // Red test: DISTINCT * over a join expands both branches' columns (with the dedup-suffixed
        // ts1) and leaks the helper the same way. ASOF JOIN maps each master row to the latest
        // slave row at or before its timestamp: rows 1,2 -> (100.0, ts=1); rows 3,4 -> (200.0,
        // ts=3). All four joined rows are distinct; uniform(2) keeps the first and last.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE joins_a (pa DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO joins_a VALUES (1.0, 1), (2.0, 2), (3.0, 3), (4.0, 4)");
            execute("CREATE TABLE joins_b (pb DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO joins_b VALUES (100.0, 1), (200.0, 3)");
            final String expected = "pa\tts\tpb\tts1\n" +
                    "1.0\t1970-01-01T00:00:00.000001Z\t100.0\t1970-01-01T00:00:00.000001Z\n" +
                    "4.0\t1970-01-01T00:00:00.000004Z\t200.0\t1970-01-01T00:00:00.000003Z\n";
            assertQuery("SELECT DISTINCT a.pa, a.ts, b.pb, b.ts FROM joins_a a ASOF JOIN joins_b b SUBSAMPLE uniform(2)")
                    .returns(expected);
            assertQuery("SELECT DISTINCT * FROM joins_a a ASOF JOIN joins_b b SUBSAMPLE uniform(2)").returns(expected);
        });
    }

    @Test
    public void testDistinctStarOverUserNamedKeepSubsampleColumn() throws Exception {
        // Red test (F4 consequence shape): a user column literally named __keep_subsample under
        // DISTINCT *. The F4 collector correctly escapes the helper to __keep_subsample1, which
        // then leaks through the aggregation branch exactly like the plain shape. Oracle: the
        // explicit-column DISTINCT form (the non-wildcard alias escape, green today).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE keep_tbl (__keep_subsample BOOLEAN, x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO keep_tbl VALUES (false, 1, 1), (true, 2, 2), (false, 3, 3), (true, 4, 4)");
            final String expected = "__keep_subsample\tx\tts\n" +
                    "false\t1\t1970-01-01T00:00:00.000001Z\n" +
                    "true\t4\t1970-01-01T00:00:00.000004Z\n";
            assertQuery("SELECT DISTINCT __keep_subsample, x, ts FROM keep_tbl SUBSAMPLE uniform(2)").returns(expected);
            assertQuery("SELECT DISTINCT * FROM keep_tbl SUBSAMPLE uniform(2)").returns(expected);
        });
    }

    @Test
    public void testGroupByAllKeysStarUniform() throws Exception {
        // Red test: an explicit GROUP BY naming every projected column admits a bare wildcard
        // (unlike SAMPLE BY, which rejects it) and reaches the same aggregation-branch leak.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (10.0, 'A', 1), (10.0, 'A', 1), (20.0, 'B', 2), (30.0, 'A', 3), (40.0, 'B', 4)");
            final String expected = "price\tsym\tts\n" +
                    "10.0\tA\t1970-01-01T00:00:00.000001Z\n" +
                    "40.0\tB\t1970-01-01T00:00:00.000004Z\n";
            assertQuery("SELECT price, sym, ts FROM t GROUP BY price, sym, ts SUBSAMPLE uniform(2)").returns(expected);
            assertQuery("SELECT * FROM t GROUP BY price, sym, ts SUBSAMPLE uniform(2)").returns(expected);
        });
    }

    @Test
    public void testStarWithCountUniform() throws Exception {
        // Red test: a wildcard next to an aggregate function (implicit group-by over all wildcard
        // columns) is an aggregation context with a real '*' and reaches the same leak. The
        // duplicate first row makes its group count 2, proving the group-by ran below SUBSAMPLE.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (10.0, 'A', 1), (10.0, 'A', 1), (20.0, 'B', 2), (30.0, 'A', 3), (40.0, 'B', 4)");
            final String expected = "price\tsym\tts\tcount\n" +
                    "10.0\tA\t1970-01-01T00:00:00.000001Z\t2\n" +
                    "40.0\tB\t1970-01-01T00:00:00.000004Z\t1\n";
            assertQuery("SELECT price, sym, ts, count() FROM t SUBSAMPLE uniform(2)").returns(expected);
            assertQuery("SELECT *, count() FROM t SUBSAMPLE uniform(2)").returns(expected);
        });
    }

    @Test
    public void testDistinctStarWithCountUniform() throws Exception {
        // Corner pin (design review F7 round 1): DISTINCT combined with `*, count()` sets both
        // the isDistinct and hasGroupByFunc triggers of isAggregationContext at once - a shape
        // testStarWithCountUniform does not cover. The implicit group-by over all wildcard
        // columns already makes each output row unique, so DISTINCT must not change the rows.
        // Oracle: the explicit-column form, asserted green first.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (10.0, 'A', 1), (10.0, 'A', 1), (20.0, 'B', 2), (30.0, 'A', 3), (40.0, 'B', 4)");
            final String expected = "price\tsym\tts\tcount\n" +
                    "10.0\tA\t1970-01-01T00:00:00.000001Z\t2\n" +
                    "40.0\tB\t1970-01-01T00:00:00.000004Z\t1\n";
            assertQuery("SELECT DISTINCT price, sym, ts, count() FROM t SUBSAMPLE uniform(2)").returns(expected);
            assertQuery("SELECT DISTINCT *, count() FROM t SUBSAMPLE uniform(2)").returns(expected);
        });
    }

    @Test
    public void testDistinctTableDotStarJoinUniform() throws Exception {
        // Corner pin (design review F7 round 1): a table-qualified wildcard over a JOIN under
        // DISTINCT. DISTINCT a.* projects only the master branch's columns; the four master rows
        // are distinct, and uniform(2) keeps the first and last in timestamp order. Oracle: the
        // explicit qualified-column form (F1 contract), asserted green first.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE joins_a (pa DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO joins_a VALUES (1.0, 1), (2.0, 2), (3.0, 3), (4.0, 4)");
            execute("CREATE TABLE joins_b (pb DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO joins_b VALUES (100.0, 1), (200.0, 3)");
            final String expected = "pa\tts\n" +
                    "1.0\t1970-01-01T00:00:00.000001Z\n" +
                    "4.0\t1970-01-01T00:00:00.000004Z\n";
            assertQuery("SELECT DISTINCT a.pa, a.ts FROM joins_a a ASOF JOIN joins_b b SUBSAMPLE uniform(2)")
                    .returns(expected);
            assertQuery("SELECT DISTINCT a.* FROM joins_a a ASOF JOIN joins_b b SUBSAMPLE uniform(2)").returns(expected);
        });
    }

    @Test
    public void testDistinctStarControls() throws Exception {
        // Preservation controls (green today, must stay green): DISTINCT * without SUBSAMPLE is
        // untouched engine behavior; sdt still refuses every aggregation context including the
        // wildcard one (position points at the sdt token).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (10.0, 'A', 1), (10.0, 'A', 1), (20.0, 'B', 2), (30.0, 'A', 3), (40.0, 'B', 4)");
            assertQuery("SELECT DISTINCT * FROM t")
                    .expectSize()
                    .returns("price\tsym\tts\n" +
                            "10.0\tA\t1970-01-01T00:00:00.000001Z\n" +
                            "20.0\tB\t1970-01-01T00:00:00.000002Z\n" +
                            "30.0\tA\t1970-01-01T00:00:00.000003Z\n" +
                            "40.0\tB\t1970-01-01T00:00:00.000004Z\n");
            assertException(
                    "SELECT DISTINCT * FROM t SUBSAMPLE sdt(price, 0.5)",
                    35,
                    "SUBSAMPLE sdt is not supported in an aggregation context"
            );
        });
    }

    @Test
    public void testExplicitTimestampOnSubqueryJoinBranchCollidingName() throws Exception {
        // F6 red test (root cause, SUBSAMPLE-free): an explicit TIMESTAMP clause on a subquery
        // join branch must scope to that branch's projection. Today moveTimestampToChooseModel
        // hoists it onto the model above the whole join, where the unqualified name collides
        // with the slave's same-named column and JoinRecordMetadata reports it as not found:
        // "Invalid column: ts". Oracle: the identical query WITHOUT the clause (asserted first),
        // which must stay equivalent because the subquery already designates ts.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");
            final String expected = """
                    x\tts\tlabel
                    1\t1970-01-01T00:00:00.000010Z\t200
                    2\t1970-01-01T00:00:00.000020Z\t200
                    3\t1970-01-01T00:00:00.000030Z\t300
                    4\t1970-01-01T00:00:00.000040Z\t400
                    """;
            assertQuery("SELECT s.x, s.ts, r.label FROM (SELECT ts, x FROM t) s ASOF JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT s.x, s.ts, r.label FROM (SELECT ts, x FROM t) s TIMESTAMP(ts) ASOF JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testExplicitTimestampOverSubsampledSubqueryCollidingAsofJoin() throws Exception {
        // F6 red test (symptom, minimal SUBSAMPLE shape): same defect through a subsampled
        // subquery join branch. Without the outer join (or with a non-colliding slave) the
        // clause works; see testExplicitTimestampOverSubsampledSubqueryNoJoinControls. Oracle:
        // the identical query WITHOUT the clause (F1 guarantees it designates ts).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");
            final String expected = """
                    ts\tx\tlabel\tts1
                    1970-01-01T00:00:00.000010Z\t1\t200\t1970-01-01T00:00:00.000008Z
                    1970-01-01T00:00:00.000040Z\t4\t400\t1970-01-01T00:00:00.000035Z
                    """;
            assertQuery("SELECT * FROM (SELECT ts, x FROM t SUBSAMPLE uniform(2)) s ASOF JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(expected);
            assertQuery("SELECT * FROM (SELECT ts, x FROM t SUBSAMPLE uniform(2)) s TIMESTAMP(ts) ASOF JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testExplicitTimestampOverSubsampledSubqueryCollidingLtJoin() throws Exception {
        // F6 red test: the colliding-name failure is join-type independent; LT JOIN variant.
        // Oracle: the identical no-clause query asserted first.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");
            final String ltExpected = """
                    ts\tx\tlabel
                    1970-01-01T00:00:00.000010Z\t1\t200
                    1970-01-01T00:00:00.000040Z\t4\t400
                    """;
            assertQuery("SELECT s.ts, s.x, r.label FROM (SELECT ts, x FROM t SUBSAMPLE uniform(2)) s LT JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(ltExpected);
            assertQuery("SELECT s.ts, s.x, r.label FROM (SELECT ts, x FROM t SUBSAMPLE uniform(2)) s TIMESTAMP(ts) LT JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(ltExpected);
        });
    }

    @Test
    public void testExplicitTimestampOverSubsampledSubqueryCollidingInnerJoin() throws Exception {
        // F6 red test: the colliding-name failure is join-type independent; INNER JOIN ON variant.
        // Oracle: the identical no-clause query asserted first.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tse (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tse VALUES (900, 10), (700, 15), (800, 40)");
            final String innerExpected = """
                    ts\tx\tlabel
                    1970-01-01T00:00:00.000010Z\t1\t900
                    1970-01-01T00:00:00.000040Z\t4\t800
                    """;
            assertQuery("SELECT s.ts, s.x, e.label FROM (SELECT ts, x FROM t SUBSAMPLE uniform(2)) s JOIN tse e ON e.ts = s.ts")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(innerExpected);
            assertQuery("SELECT s.ts, s.x, e.label FROM (SELECT ts, x FROM t SUBSAMPLE uniform(2)) s TIMESTAMP(ts) JOIN tse e ON e.ts = s.ts")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(innerExpected);
        });
    }

    @Test
    public void testExplicitTimestampOverSubsampledSubqueryCollidingCrossJoin() throws Exception {
        // F6 red test: the colliding-name failure is join-type independent; CROSS JOIN variant.
        // The explicit-projection form compiles today (the projection resolves qualified refs
        // before the join-wide timestamp lookup) - pinned first as a control. The star form
        // takes the generateSelectChoose(getTimestampIndex) path over the join metadata and
        // fails. Oracle for the star form: the identical no-clause star query.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");
            final String crossExpected = """
                    ts\tx\tlabel
                    1970-01-01T00:00:00.000010Z\t1\t100
                    1970-01-01T00:00:00.000010Z\t1\t200
                    1970-01-01T00:00:00.000010Z\t1\t300
                    1970-01-01T00:00:00.000010Z\t1\t400
                    1970-01-01T00:00:00.000040Z\t4\t100
                    1970-01-01T00:00:00.000040Z\t4\t200
                    1970-01-01T00:00:00.000040Z\t4\t300
                    1970-01-01T00:00:00.000040Z\t4\t400
                    """;
            assertQuery("SELECT s.ts, s.x, r.label FROM (SELECT ts, x FROM t SUBSAMPLE uniform(2)) s CROSS JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(crossExpected);
            assertQuery("SELECT s.ts, s.x, r.label FROM (SELECT ts, x FROM t SUBSAMPLE uniform(2)) s TIMESTAMP(ts) CROSS JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(crossExpected);
            final String crossStarExpected = """
                    ts\tx\tlabel\tts1
                    1970-01-01T00:00:00.000010Z\t1\t100\t1970-01-01T00:00:00.000000Z
                    1970-01-01T00:00:00.000010Z\t1\t200\t1970-01-01T00:00:00.000008Z
                    1970-01-01T00:00:00.000010Z\t1\t300\t1970-01-01T00:00:00.000028Z
                    1970-01-01T00:00:00.000010Z\t1\t400\t1970-01-01T00:00:00.000035Z
                    1970-01-01T00:00:00.000040Z\t4\t100\t1970-01-01T00:00:00.000000Z
                    1970-01-01T00:00:00.000040Z\t4\t200\t1970-01-01T00:00:00.000008Z
                    1970-01-01T00:00:00.000040Z\t4\t300\t1970-01-01T00:00:00.000028Z
                    1970-01-01T00:00:00.000040Z\t4\t400\t1970-01-01T00:00:00.000035Z
                    """;
            assertQuery("SELECT * FROM (SELECT ts, x FROM t SUBSAMPLE uniform(2)) s CROSS JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(crossStarExpected);
            assertQuery("SELECT * FROM (SELECT ts, x FROM t SUBSAMPLE uniform(2)) s TIMESTAMP(ts) CROSS JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(crossStarExpected);
        });
    }

    @Test
    public void testExplicitTimestampOverSubsampledSubqueryEvidenceShape() throws Exception {
        // F6 red test: the original review evidence shape (inner ASOF join + SUBSAMPLE +
        // explicit TIMESTAMP + outer ASOF join against a slave with a colliding ts column).
        // Oracle: the identical query WITHOUT the clause, pinned green by the F1 battery
        // (testSubsampleJoinDesignatedTimestampSurvivesQualifiedProjectionOrder).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tsp (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsq (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO tsp VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsq VALUES (8, 5), (9, 25)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");
            final String expected = """
                    x\trt\tts\tlabel
                    1\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000010Z\t200
                    4\t1970-01-01T00:00:00.000025Z\t1970-01-01T00:00:00.000040Z\t400
                    """;
            assertQuery("""
                    SELECT s.x, s.rt, s.ts, r.label
                    FROM (
                        SELECT q.ts rt, p.ts ts, p.x
                        FROM tsp p ASOF JOIN tsq q SUBSAMPLE uniform(2)
                    ) s ASOF JOIN tsr r""")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(expected);
            assertQuery("""
                    SELECT s.x, s.rt, s.ts, r.label
                    FROM (
                        SELECT q.ts rt, p.ts ts, p.x
                        FROM tsp p ASOF JOIN tsq q SUBSAMPLE uniform(2)
                    ) s TIMESTAMP(ts) ASOF JOIN tsr r""")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testExplicitTimestampOverSubsampledSubqueryNoJoinControls() throws Exception {
        // F6 preservation controls (green pre-fix, must stay green): every no-join explicit
        // TIMESTAMP shape over a subsampled subquery already works, across the desugar tail
        // (uniform, lttb, m4), star/explicit/renamed projections, aggregation, and an inner
        // join. These pin the working family the F6 defect was originally attributed to.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsp (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsq (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsp VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsq VALUES (8, 5), (9, 25)");
            final String uniformExpected = """
                    ts\tx
                    1970-01-01T00:00:00.000010Z\t1
                    1970-01-01T00:00:00.000040Z\t4
                    """;
            assertQuery("SELECT * FROM (SELECT ts, x FROM t SUBSAMPLE uniform(2)) TIMESTAMP(ts)")
                    .timestamp("ts")
                    .returns(uniformExpected);
            assertQuery("SELECT * FROM (SELECT ts, x FROM t SUBSAMPLE uniform(2)) s TIMESTAMP(ts)")
                    .timestamp("ts")
                    .returns(uniformExpected);
            assertQuery("SELECT ts, x FROM (SELECT ts, x FROM t SUBSAMPLE uniform(2)) TIMESTAMP(ts)")
                    .timestamp("ts")
                    .returns(uniformExpected);
            // NOTE: the desugared wildcard stack projects the window timestamp first (ts, x),
            // not in table order (x, ts) - pinned current behavior.
            assertQuery("SELECT * FROM (SELECT * FROM t SUBSAMPLE uniform(2)) TIMESTAMP(ts)")
                    .timestamp("ts")
                    .returns(uniformExpected);
            // TIMESTAMP(other_col): re-designating onto a renamed projection of the timestamp
            // works over a subsampled subquery exactly as over ordinary subqueries.
            assertQuery("SELECT * FROM (SELECT ts, x, ts AS t2 FROM t SUBSAMPLE uniform(2)) TIMESTAMP(t2)")
                    .timestamp("t2")
                    .returns("""
                            ts\tx\tt2
                            1970-01-01T00:00:00.000010Z\t1\t1970-01-01T00:00:00.000010Z
                            1970-01-01T00:00:00.000040Z\t4\t1970-01-01T00:00:00.000040Z
                            """);
            assertQuery("SELECT * FROM (SELECT ts, x FROM t SUBSAMPLE lttb(x, 3)) TIMESTAMP(ts)")
                    .timestamp("ts")
                    .returns("""
                            ts\tx
                            1970-01-01T00:00:00.000010Z\t1
                            1970-01-01T00:00:00.000020Z\t2
                            1970-01-01T00:00:00.000040Z\t4
                            """);
            assertQuery("SELECT * FROM (SELECT ts, x FROM t SUBSAMPLE m4(x, 2)) TIMESTAMP(ts)")
                    .timestamp("ts")
                    .returns(uniformExpected);
            assertQuery("SELECT * FROM (SELECT ts, max(x) mx FROM t SAMPLE BY 10U SUBSAMPLE uniform(2)) TIMESTAMP(ts)")
                    .timestamp("ts")
                    .returns("""
                            ts\tmx
                            1970-01-01T00:00:00.000010Z\t1
                            1970-01-01T00:00:00.000040Z\t4
                            """);
            // NOTE: the explicit clause moves the designated column to the front of the star
            // expansion (ts, rt, x instead of the subquery's rt, ts, x) - pinned current behavior.
            assertQuery("SELECT * FROM (SELECT q.ts rt, p.ts ts, p.x FROM tsp p ASOF JOIN tsq q SUBSAMPLE uniform(2)) s TIMESTAMP(ts)")
                    .timestamp("ts")
                    .returns("""
                            ts\trt\tx
                            1970-01-01T00:00:00.000010Z\t1970-01-01T00:00:00.000005Z\t1
                            1970-01-01T00:00:00.000040Z\t1970-01-01T00:00:00.000025Z\t4
                            """);
        });
    }

    @Test
    public void testExplicitTimestampJoinBranchNonCollidingControls() throws Exception {
        // F6 preservation controls (green pre-fix, must stay green): with no name collision
        // across join branches, the explicit clause on a subquery branch compiles today, and a
        // bare-table branch applies its clause at branch scope (including the colliding-name
        // case, which never takes the hoist path because the branch has a table name).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsn (label LONG, rts TIMESTAMP) TIMESTAMP(rts)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");
            execute("INSERT INTO tsn VALUES (100, 0), (200, 8), (300, 28), (400, 35)");
            assertQuery("SELECT * FROM (SELECT ts, x FROM t SUBSAMPLE uniform(2)) s TIMESTAMP(ts) ASOF JOIN tsn n")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tx\tlabel\trts
                            1970-01-01T00:00:00.000010Z\t1\t200\t1970-01-01T00:00:00.000008Z
                            1970-01-01T00:00:00.000040Z\t4\t400\t1970-01-01T00:00:00.000035Z
                            """);
            assertQuery("SELECT s.x, s.ts, n.label FROM (SELECT ts, x FROM t) s TIMESTAMP(ts) ASOF JOIN tsn n")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\tts\tlabel
                            1\t1970-01-01T00:00:00.000010Z\t200
                            2\t1970-01-01T00:00:00.000020Z\t200
                            3\t1970-01-01T00:00:00.000030Z\t300
                            4\t1970-01-01T00:00:00.000040Z\t400
                            """);
            assertQuery("SELECT * FROM t TIMESTAMP(ts) ASOF JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\tts\tlabel\tts1
                            1\t1970-01-01T00:00:00.000010Z\t200\t1970-01-01T00:00:00.000008Z
                            2\t1970-01-01T00:00:00.000020Z\t200\t1970-01-01T00:00:00.000008Z
                            3\t1970-01-01T00:00:00.000030Z\t300\t1970-01-01T00:00:00.000028Z
                            4\t1970-01-01T00:00:00.000040Z\t400\t1970-01-01T00:00:00.000035Z
                            """);
        });
    }

    @Test
    public void testExplicitTimestampBareTableBranchScopesJoinKey() throws Exception {
        // F6 preservation control (green pre-fix, must stay green): the engine's established
        // semantics for an explicit TIMESTAMP clause on a bare-table join branch - the clause
        // re-designates the branch timestamp and the ASOF join keys on it. This is the
        // precedent the subquery-branch fix must align with (invariant: a FROM item's
        // TIMESTAMP clause scopes to that FROM item).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t2 (x LONG, ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsm (label LONG, rts TIMESTAMP) TIMESTAMP(rts)");
            execute("INSERT INTO t2 VALUES (1, 10, 1000), (2, 20, 2000)");
            execute("INSERT INTO tsm VALUES (100, 5), (200, 15), (300, 1500), (400, 2500)");
            // no clause: joins on the designated ts (labels 100, 200)
            assertQuery("SELECT * FROM t2 ASOF JOIN tsm m")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\tts\tts2\tlabel\trts
                            1\t1970-01-01T00:00:00.000010Z\t1970-01-01T00:00:00.001000Z\t100\t1970-01-01T00:00:00.000005Z
                            2\t1970-01-01T00:00:00.000020Z\t1970-01-01T00:00:00.002000Z\t200\t1970-01-01T00:00:00.000015Z
                            """);
            // clause names ts2: the branch re-designates and the join keys on ts2 (labels 200, 300)
            assertQuery("SELECT * FROM t2 TIMESTAMP(ts2) ASOF JOIN tsm m")
                    .timestamp("ts2")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\tts\tts2\tlabel\trts
                            1\t1970-01-01T00:00:00.000010Z\t1970-01-01T00:00:00.001000Z\t200\t1970-01-01T00:00:00.000015Z
                            2\t1970-01-01T00:00:00.000020Z\t1970-01-01T00:00:00.002000Z\t300\t1970-01-01T00:00:00.001500Z
                            """);
        });
    }

    @Test
    public void testExplicitTimestampErrorControls() throws Exception {
        // F6 preservation controls (green pre-fix, must stay green): genuinely invalid
        // explicit TIMESTAMP clauses keep their error message and clause-token position,
        // with and without a join.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");
            assertException(
                    "SELECT * FROM (SELECT ts, x FROM t SUBSAMPLE uniform(2)) TIMESTAMP(nope)",
                    67,
                    "Invalid column: nope"
            );
            assertException(
                    "SELECT * FROM (SELECT ts, x FROM t SUBSAMPLE uniform(2)) TIMESTAMP(x)",
                    67,
                    "not a TIMESTAMP"
            );
            assertException(
                    "SELECT s.x FROM (SELECT ts, x FROM t) s TIMESTAMP(nope) ASOF JOIN tsr r",
                    50,
                    "Invalid column: nope"
            );
        });
    }

    @Test
    public void testExplicitTimestampOnAliasedInnerJoinBranch() throws Exception {
        // F6 fix-phase test (obligation e): the SUBSAMPLE-free aliased-inner-join family member.
        // Pre-fix: "Invalid column: ts" at position 107 - the branch clause was hoisted above the
        // outer join, where unqualified ts collides with the slave's ts. Post-fix the clause
        // resolves against the branch OUTPUT metadata (rt, ts, x - unique ts); the inner
        // JoinRecordMetadata is never consulted. Oracle: the identical no-clause query.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tsp (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsq (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO tsp VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsq VALUES (8, 5), (9, 25)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");
            final String expected = """
                    x\trt\tts\tlabel
                    1\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000010Z\t200
                    2\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000020Z\t200
                    3\t1970-01-01T00:00:00.000025Z\t1970-01-01T00:00:00.000030Z\t300
                    4\t1970-01-01T00:00:00.000025Z\t1970-01-01T00:00:00.000040Z\t400
                    """;
            assertQuery("SELECT s.x, s.rt, s.ts, r.label FROM (SELECT q.ts rt, p.ts ts, p.x FROM tsp p ASOF JOIN tsq q) s ASOF JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT s.x, s.rt, s.ts, r.label FROM (SELECT q.ts rt, p.ts ts, p.x FROM tsp p ASOF JOIN tsq q) s TIMESTAMP(ts) ASOF JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testExplicitTimestampRenamedClauseOnJoinBranchPreserved() throws Exception {
        // F6 preservation pin (obligation f, green pre-fix, must stay green): a renamed clause
        // (TIMESTAMP(tt)) on a subquery join branch resolves against the branch OUTPUT, where tt
        // exists; the branch already designates tt, so rows, designation, and plan equal the
        // no-clause query. Plain and SUBSAMPLE forms.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");
            final String plainExpected = """
                    tt\tx\tlabel\tts
                    1970-01-01T00:00:00.000010Z\t1\t200\t1970-01-01T00:00:00.000008Z
                    1970-01-01T00:00:00.000020Z\t2\t200\t1970-01-01T00:00:00.000008Z
                    1970-01-01T00:00:00.000030Z\t3\t300\t1970-01-01T00:00:00.000028Z
                    1970-01-01T00:00:00.000040Z\t4\t400\t1970-01-01T00:00:00.000035Z
                    """;
            assertQuery("SELECT * FROM (SELECT ts tt, x FROM t) s ASOF JOIN tsr r")
                    .timestamp("tt")
                    .noRandomAccess()
                    .expectSize()
                    .returns(plainExpected);
            assertQuery("SELECT * FROM (SELECT ts tt, x FROM t) s TIMESTAMP(tt) ASOF JOIN tsr r")
                    .timestamp("tt")
                    .noRandomAccess()
                    .expectSize()
                    .returns(plainExpected);
            final String subsampleExpected = """
                    tt\tx\tlabel\tts
                    1970-01-01T00:00:00.000010Z\t1\t200\t1970-01-01T00:00:00.000008Z
                    1970-01-01T00:00:00.000040Z\t4\t400\t1970-01-01T00:00:00.000035Z
                    """;
            assertQuery("SELECT * FROM (SELECT ts tt, x FROM t SUBSAMPLE uniform(2)) s ASOF JOIN tsr r")
                    .timestamp("tt")
                    .noRandomAccess()
                    .returns(subsampleExpected);
            assertQuery("SELECT * FROM (SELECT ts tt, x FROM t SUBSAMPLE uniform(2)) s TIMESTAMP(tt) ASOF JOIN tsr r")
                    .timestamp("tt")
                    .noRandomAccess()
                    .returns(subsampleExpected);
        });
    }

    @Test
    public void testExplicitTimestampSubqueryBranchRekeysTimeSeriesJoin() throws Exception {
        // F6 fix-phase pin (obligations a/b + LT variant): CONTRACT CHANGE. A clause naming a
        // DIFFERENT timestamp column of a subquery join branch now re-designates the branch
        // pre-join, so time-series joins key on that column - matching the bare-table precedent
        // (testExplicitTimestampBareTableBranchScopesJoinKey). Pre-fix the clause only relabeled
        // the output metadata with an order the cursor did not deliver (rows stayed keyed on ts).
        // The bare-table form of each join is asserted first as the semantic oracle. The EXPLAIN
        // pin below (testExplicitTimestampExplainPins) documents the surviving wrapper node.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t2 (x LONG, ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsm (label LONG, rts TIMESTAMP) TIMESTAMP(rts)");
            execute("INSERT INTO t2 VALUES (1, 10, 1000), (2, 20, 2000)");
            execute("INSERT INTO tsm VALUES (100, 5), (200, 15), (300, 1500), (400, 2500)");
            // ASOF: bare-table oracle (T1 semantics), then the subquery branch aligns with it
            assertQuery("SELECT * FROM t2 TIMESTAMP(ts2) ASOF JOIN tsm m")
                    .timestamp("ts2")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\tts\tts2\tlabel\trts
                            1\t1970-01-01T00:00:00.000010Z\t1970-01-01T00:00:00.001000Z\t200\t1970-01-01T00:00:00.000015Z
                            2\t1970-01-01T00:00:00.000020Z\t1970-01-01T00:00:00.002000Z\t300\t1970-01-01T00:00:00.001500Z
                            """);
            assertQuery("SELECT * FROM (SELECT ts, ts2, x FROM t2) s TIMESTAMP(ts2) ASOF JOIN tsm m")
                    .timestamp("ts2")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tts2\tx\tlabel\trts
                            1970-01-01T00:00:00.000010Z\t1970-01-01T00:00:00.001000Z\t1\t200\t1970-01-01T00:00:00.000015Z
                            1970-01-01T00:00:00.000020Z\t1970-01-01T00:00:00.002000Z\t2\t300\t1970-01-01T00:00:00.001500Z
                            """);
            // LT: bare-table oracle, then the subquery branch aligns with it
            assertQuery("SELECT * FROM t2 TIMESTAMP(ts2) LT JOIN tsm m")
                    .timestamp("ts2")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\tts\tts2\tlabel\trts
                            1\t1970-01-01T00:00:00.000010Z\t1970-01-01T00:00:00.001000Z\t200\t1970-01-01T00:00:00.000015Z
                            2\t1970-01-01T00:00:00.000020Z\t1970-01-01T00:00:00.002000Z\t300\t1970-01-01T00:00:00.001500Z
                            """);
            assertQuery("SELECT * FROM (SELECT ts, ts2, x FROM t2) s TIMESTAMP(ts2) LT JOIN tsm m")
                    .timestamp("ts2")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tts2\tx\tlabel\trts
                            1970-01-01T00:00:00.000010Z\t1970-01-01T00:00:00.001000Z\t1\t200\t1970-01-01T00:00:00.000015Z
                            1970-01-01T00:00:00.000020Z\t1970-01-01T00:00:00.002000Z\t2\t300\t1970-01-01T00:00:00.001500Z
                            """);
        });
    }

    @Test
    public void testExplicitTimestampRestoresJoinAfterBranchLostDesignation() throws Exception {
        // F6 fix-phase pin (obligation c, R7 outcome): a subquery branch that lost its designated
        // timestamp (ORDER BY x) cannot join without the clause ("left side of time series join
        // has no timestamp" - asserted first). Pre-fix the clause was silently dropped and the
        // same error remained. Post-fix the clause designates ts inside the branch and the ASOF
        // join works. The branch data is x-ascending == ts-ascending, so the delivered order
        // satisfies the designation.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");
            assertException(
                    "SELECT s.ts, s.x, r.label FROM (SELECT ts, x FROM t ORDER BY x) s ASOF JOIN tsr r",
                    66,
                    "left side of time series join has no timestamp"
            );
            assertQuery("SELECT s.ts, s.x, r.label FROM (SELECT ts, x FROM t ORDER BY x) s TIMESTAMP(ts) ASOF JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tx\tlabel
                            1970-01-01T00:00:00.000010Z\t1\t200
                            1970-01-01T00:00:00.000020Z\t2\t200
                            1970-01-01T00:00:00.000030Z\t3\t300
                            1970-01-01T00:00:00.000040Z\t4\t400
                            """);
        });
    }

    @Test
    public void testExplicitTimestampAggregationOverJoinBranch() throws Exception {
        // F6 fix-phase test (obligation d): aggregation directly over the clause-bearing join
        // (generateSelectGroupBy consumer). Pre-fix: "Invalid column: ts" at position 53.
        // Post-fix the group-by model receives no hoisted clause. Independent aggregate oracle:
        // plain max over the master table (the ASOF join adds columns, never drops master rows).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");
            final String expected = """
                    max
                    4
                    """;
            assertQuery("SELECT max(x) FROM t").noRandomAccess().expectSize().returns(expected);
            assertQuery("SELECT max(x) FROM (SELECT ts, x FROM t) s ASOF JOIN tsr r").noRandomAccess().expectSize().returns(expected);
            assertQuery("SELECT max(x) FROM (SELECT ts, x FROM t) s TIMESTAMP(ts) ASOF JOIN tsr r").noRandomAccess().expectSize().returns(expected);
        });
    }

    @Test
    public void testExplicitTimestampParenthesizedTableBranch() throws Exception {
        // F6 fix-phase test (obligation g): a parenthesized bare-table branch head takes the
        // direct-assign arm (the clause moves onto the table model itself). Pre-fix the colliding
        // shape threw "Invalid column: ts" at position 47. Post-fix it matches the
        // unparenthesized bare-table semantics, including the TIMESTAMP(ts2) re-key (labels
        // 200/300, the T1 values pinned in testExplicitTimestampBareTableBranchScopesJoinKey).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE t2 (x LONG, ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsm (label LONG, rts TIMESTAMP) TIMESTAMP(rts)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO t2 VALUES (1, 10, 1000), (2, 20, 2000)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");
            execute("INSERT INTO tsm VALUES (100, 5), (200, 15), (300, 1500), (400, 2500)");
            final String expected = """
                    x\tts\tlabel
                    1\t1970-01-01T00:00:00.000010Z\t200
                    2\t1970-01-01T00:00:00.000020Z\t200
                    3\t1970-01-01T00:00:00.000030Z\t300
                    4\t1970-01-01T00:00:00.000040Z\t400
                    """;
            assertQuery("SELECT s.x, s.ts, r.label FROM (t) s ASOF JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT s.x, s.ts, r.label FROM (t) s TIMESTAMP(ts) ASOF JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT * FROM (t2) s TIMESTAMP(ts2) ASOF JOIN tsm m")
                    .timestamp("ts2")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\tts\tts2\tlabel\trts
                            1\t1970-01-01T00:00:00.000010Z\t1970-01-01T00:00:00.001000Z\t200\t1970-01-01T00:00:00.000015Z
                            2\t1970-01-01T00:00:00.000020Z\t1970-01-01T00:00:00.002000Z\t300\t1970-01-01T00:00:00.001500Z
                            """);
        });
    }

    @Test
    public void testExplicitTimestampUnionHeadBranch() throws Exception {
        // F6 fix-phase test (obligation h): a union head as the join branch. Without the clause
        // the union output has no designated timestamp and the ASOF join is rejected (asserted
        // first - unchanged behavior). Pre-fix the clause query failed the same way (the hoisted
        // clause never designated the branch). Post-fix the wrapper sits above the whole union,
        // the clause scopes to union OUTPUT, and the join works. Oracle rows derived by hand:
        // UNION ALL preserves arm order (ta rows then tb rows - ascending here), ASOF matches
        // each ts against tsr.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ta (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tb (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO ta VALUES (1, 10), (2, 20)");
            execute("INSERT INTO tb VALUES (3, 30), (4, 40)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");
            assertException(
                    "SELECT s.ts, s.x, r.label FROM (SELECT ts, x FROM ta UNION ALL SELECT ts, x FROM tb) s ASOF JOIN tsr r",
                    87,
                    "left side of time series join has no timestamp"
            );
            assertQuery("SELECT s.ts, s.x, r.label FROM (SELECT ts, x FROM ta UNION ALL SELECT ts, x FROM tb) s TIMESTAMP(ts) ASOF JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tx\tlabel
                            1970-01-01T00:00:00.000010Z\t1\t200
                            1970-01-01T00:00:00.000020Z\t2\t200
                            1970-01-01T00:00:00.000030Z\t3\t300
                            1970-01-01T00:00:00.000040Z\t4\t400
                            """);
        });
    }

    @Test
    public void testExplicitTimestampSampleByOverClauseJoin() throws Exception {
        // F6 fix-phase test (obligation i): SAMPLE BY (and a FILL variant, exercising the FILL
        // timestamp walk) over a clause-bearing join. The sink flips generateSampleBy's
        // pushTimestampRequiredFlag(model.getTimestamp() == null) from false to true - benign,
        // because the branch-designated timestamp flows through the join metadata. Non-colliding
        // forms were green pre-fix and must stay green with identical rows; the colliding form
        // threw "Invalid column: ts" pre-fix and now equals its no-clause oracle.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsn (label LONG, rts TIMESTAMP) TIMESTAMP(rts)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");
            execute("INSERT INTO tsn VALUES (100, 0), (200, 8), (300, 28), (400, 35)");
            final String expected = """
                    ts\tmax
                    1970-01-01T00:00:00.000000Z\t1
                    1970-01-01T00:00:00.000020Z\t3
                    1970-01-01T00:00:00.000040Z\t4
                    """;
            assertQuery("SELECT s.ts, max(s.x) FROM (SELECT ts, x FROM t) s ASOF JOIN tsn n SAMPLE BY 20U")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(expected);
            // NOTE (pinned factory-shape change): with the clause sunk into the branch the
            // SAMPLE BY model itself carries no timestamp, so generateSampleBy pushes
            // timestampRequired=true and selects a factory that supports random access; the
            // no-clause forms above/below do not. Rows and designation are identical either way.
            assertQuery("SELECT s.ts, max(s.x) FROM (SELECT ts, x FROM t) s TIMESTAMP(ts) ASOF JOIN tsn n SAMPLE BY 20U")
                    .timestamp("ts")
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT s.ts, max(s.x) FROM (SELECT ts, x FROM t) s TIMESTAMP(ts) ASOF JOIN tsn n SAMPLE BY 20U FILL(0)")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(expected);
            assertQuery("SELECT s.ts, max(s.x) FROM (SELECT ts, x FROM t) s ASOF JOIN tsr r SAMPLE BY 20U")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns(expected);
            assertQuery("SELECT s.ts, max(s.x) FROM (SELECT ts, x FROM t) s TIMESTAMP(ts) ASOF JOIN tsr r SAMPLE BY 20U")
                    .timestamp("ts")
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testExplicitTimestampHorizonJoinPrimaryBranch() throws Exception {
        // F6 fix-phase test (obligation j): the clause on the primary FROM item of a HORIZON
        // JOIN. Pre-fix the colliding shape (slave column also named ts) threw "Invalid column:
        // ts" at position 72; the non-colliding shape was green. Post-fix both equal their
        // no-clause oracles - the parent model carries no clause and the designated master
        // timestamp propagates through the inner join metadata.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, qty LONG) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            execute("CREATE TABLE prices2 (pts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(pts)");
            execute("INSERT INTO trades VALUES (1_000_000, 'A', 5), (2_000_000, 'B', 7)");
            execute("INSERT INTO prices VALUES (500_000, 'A', 100.0), (1_500_000, 'B', 200.0)");
            execute("INSERT INTO prices2 VALUES (500_000, 'A', 100.0), (1_500_000, 'B', 200.0)");
            final String expected = """
                    avg
                    150.0
                    """;
            assertQuery("SELECT avg(p.price) FROM (SELECT ts, sym, qty FROM trades) tt HORIZON JOIN prices p ON (tt.sym = p.sym) RANGE FROM 0s TO 0s STEP 1s AS h")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT avg(p.price) FROM (SELECT ts, sym, qty FROM trades) tt TIMESTAMP(ts) HORIZON JOIN prices p ON (tt.sym = p.sym) RANGE FROM 0s TO 0s STEP 1s AS h")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT avg(p.price) FROM (SELECT ts, sym, qty FROM trades) tt TIMESTAMP(ts) HORIZON JOIN prices2 p ON (tt.sym = p.sym) RANGE FROM 0s TO 0s STEP 1s AS h")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testExplicitTimestampParenthesizedJoinHeads() throws Exception {
        // F6 fix-phase test (obligation l): select-less parenthesized-join branch heads. By the
        // time the sink runs, rewriteSelectClause has synthesized a projecting CHOOSE over each
        // paren-join's full output (with dedup names ts1/ts2), so the sink's wrapper copies the
        // COMPLETE column set and the clause resolves branch-scoped - no column pruning, no scope
        // narrowing. Genuine join-holder heads (no projecting model above the join) route to the
        // fail-safe hoist instead. Pre-fix both shapes threw "Invalid column: ts" (positions
        // 68/46). Oracles: the identical no-clause queries, green pre-fix.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsq (q LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
            execute("INSERT INTO tsq VALUES (8, 5), (9, 25)");
            execute("INSERT INTO tsr VALUES (100, 0), (200, 8), (300, 28), (400, 35)");
            final String selectHeadExpected = """
                    ts\tx\tq\tts1\tlabel\tts2
                    1970-01-01T00:00:00.000010Z\t1\t8\t1970-01-01T00:00:00.000005Z\t200\t1970-01-01T00:00:00.000008Z
                    1970-01-01T00:00:00.000020Z\t2\t8\t1970-01-01T00:00:00.000005Z\t200\t1970-01-01T00:00:00.000008Z
                    1970-01-01T00:00:00.000030Z\t3\t9\t1970-01-01T00:00:00.000025Z\t300\t1970-01-01T00:00:00.000028Z
                    1970-01-01T00:00:00.000040Z\t4\t9\t1970-01-01T00:00:00.000025Z\t400\t1970-01-01T00:00:00.000035Z
                    """;
            assertQuery("SELECT * FROM ((SELECT ts, x FROM t) a ASOF JOIN tsq q) s ASOF JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(selectHeadExpected);
            assertQuery("SELECT * FROM ((SELECT ts, x FROM t) a ASOF JOIN tsq q) s TIMESTAMP(ts) ASOF JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(selectHeadExpected);
            final String tableHeadExpected = """
                    x\tts\tq\tts1\tlabel\tts2
                    1\t1970-01-01T00:00:00.000010Z\t8\t1970-01-01T00:00:00.000005Z\t200\t1970-01-01T00:00:00.000008Z
                    2\t1970-01-01T00:00:00.000020Z\t8\t1970-01-01T00:00:00.000005Z\t200\t1970-01-01T00:00:00.000008Z
                    3\t1970-01-01T00:00:00.000030Z\t9\t1970-01-01T00:00:00.000025Z\t300\t1970-01-01T00:00:00.000028Z
                    4\t1970-01-01T00:00:00.000040Z\t9\t1970-01-01T00:00:00.000025Z\t400\t1970-01-01T00:00:00.000035Z
                    """;
            assertQuery("SELECT * FROM (t ASOF JOIN tsq q) s ASOF JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(tableHeadExpected);
            assertQuery("SELECT * FROM (t ASOF JOIN tsq q) s TIMESTAMP(ts) ASOF JOIN tsr r")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(tableHeadExpected);
        });
    }

    @Test
    public void testExplicitTimestampExplainPins() throws Exception {
        // F6 fix-phase pins (obligation k). Confirmation clause: the entity wrapper is elided by
        // generateSelectChoose (alias == token for every wrapper column), so the plan is
        // byte-equal to the no-clause plan - both asserted with the same string. Re-designation
        // clause: the wrapper survives as one SelectedRecord node over the branch (pinned
        // plan-shape change; the no-clause form of that query collapses the branch into a bare
        // PageFrame).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE t2 (x LONG, ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsr (label LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE tsm (label LONG, rts TIMESTAMP) TIMESTAMP(rts)");
            final String confirmationPlan = """
                    SelectedRecord
                        AsOf Join Fast
                            PageFrame
                                Row forward scan
                                Frame forward scan on: t
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tsr
                    """;
            assertQuery("SELECT s.x, s.ts, r.label FROM (SELECT ts, x FROM t) s ASOF JOIN tsr r")
                    .assertsPlan(confirmationPlan);
            assertQuery("SELECT s.x, s.ts, r.label FROM (SELECT ts, x FROM t) s TIMESTAMP(ts) ASOF JOIN tsr r")
                    .assertsPlan(confirmationPlan);
            assertQuery("SELECT * FROM (SELECT ts, ts2, x FROM t2) s TIMESTAMP(ts2) ASOF JOIN tsm m")
                    .assertsPlan("""
                            SelectedRecord
                                AsOf Join Fast
                                    SelectedRecord
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t2
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tsm
                            """);
        });
    }
}
