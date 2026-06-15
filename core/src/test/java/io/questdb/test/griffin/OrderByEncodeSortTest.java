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
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Numbers;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class OrderByEncodeSortTest extends AbstractCairoTest {
    private final SortMode sortMode;

    public OrderByEncodeSortTest(SortMode sortMode) {
        this.sortMode = sortMode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {SortMode.SORT_ENABLED},
                {SortMode.DISABLED},
        });
    }

    @Override
    public void setUp() {
        node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, sortMode == SortMode.SORT_ENABLED);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_TOP_K_ENABLED, false);
        super.setUp();
    }

    @Test
    public void testOrderBy32ByteKeyBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x AS (
                                SELECT
                                    x % 3 AS a,
                                    x % 2 AS b,
                                    x / 4 AS c,
                                    x AS d
                                FROM long_sequence(12)
                            )"""
            );
            assertQuery("SELECT * FROM x ORDER BY a ASC, b DESC, c, d")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tc\td
                            0\t1\t0\t3
                            0\t1\t2\t9
                            0\t0\t1\t6
                            0\t0\t3\t12
                            1\t1\t0\t1
                            1\t1\t1\t7
                            1\t0\t1\t4
                            1\t0\t2\t10
                            2\t1\t1\t5
                            2\t1\t2\t11
                            2\t0\t0\t2
                            2\t0\t2\t8
                            """);
        });
    }

    @Test
    public void testOrderByDateColumnAscMixedValues() throws Exception {
        assertQuery("select * from x order by a asc;")
                .ddl("create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as date) as a" +
                        " from long_sequence(25)" +
                        ")")
                .expectSize()
                .returns("""
                        a
                        
                        
                        
                        
                        
                        1969-12-31T23:59:59.990Z
                        1969-12-31T23:59:59.991Z
                        1969-12-31T23:59:59.992Z
                        1969-12-31T23:59:59.993Z
                        1969-12-31T23:59:59.994Z
                        1969-12-31T23:59:59.995Z
                        1969-12-31T23:59:59.996Z
                        1969-12-31T23:59:59.997Z
                        1969-12-31T23:59:59.998Z
                        1969-12-31T23:59:59.999Z
                        1970-01-01T00:00:00.000Z
                        1970-01-01T00:00:00.001Z
                        1970-01-01T00:00:00.002Z
                        1970-01-01T00:00:00.003Z
                        1970-01-01T00:00:00.004Z
                        1970-01-01T00:00:00.005Z
                        1970-01-01T00:00:00.006Z
                        1970-01-01T00:00:00.007Z
                        1970-01-01T00:00:00.008Z
                        1970-01-01T00:00:00.009Z
                        """);
    }

    @Test
    public void testOrderByDateColumnDescMixedValues() throws Exception {
        assertQuery("select * from x order by a desc;")
                .ddl("create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as date) as a" +
                        " from long_sequence(25)" +
                        ")")
                .expectSize()
                .returns("""
                        a
                        1970-01-01T00:00:00.009Z
                        1970-01-01T00:00:00.008Z
                        1970-01-01T00:00:00.007Z
                        1970-01-01T00:00:00.006Z
                        1970-01-01T00:00:00.005Z
                        1970-01-01T00:00:00.004Z
                        1970-01-01T00:00:00.003Z
                        1970-01-01T00:00:00.002Z
                        1970-01-01T00:00:00.001Z
                        1970-01-01T00:00:00.000Z
                        1969-12-31T23:59:59.999Z
                        1969-12-31T23:59:59.998Z
                        1969-12-31T23:59:59.997Z
                        1969-12-31T23:59:59.996Z
                        1969-12-31T23:59:59.995Z
                        1969-12-31T23:59:59.994Z
                        1969-12-31T23:59:59.993Z
                        1969-12-31T23:59:59.992Z
                        1969-12-31T23:59:59.991Z
                        1969-12-31T23:59:59.990Z
                        
                        
                        
                        
                        
                        """);
    }

    @Test
    public void testOrderByDoubleEdgeCases() throws Exception {
        Assume.assumeTrue(sortMode == SortMode.SORT_ENABLED);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x AS (
                                SELECT CASE x
                                    WHEN 1 THEN CAST('NaN' AS DOUBLE)
                                    WHEN 2 THEN CAST('-Infinity' AS DOUBLE)
                                    WHEN 3 THEN CAST('Infinity' AS DOUBLE)
                                    WHEN 4 THEN 0.0
                                    WHEN 5 THEN -0.0
                                    WHEN 6 THEN 1.0000000000000002
                                    WHEN 7 THEN 1.0000000000000004
                                    WHEN 8 THEN -1.0000000000000002
                                    WHEN 9 THEN -1.0000000000000004
                                    WHEN 10 THEN CAST('NaN' AS DOUBLE)
                                    ELSE 0.5
                                END AS d
                                FROM long_sequence(11)
                            )"""
            );
            // Signed zeros tie under the canonicalizing encoder, so their order is
            // the rowId tiebreak: 0.0 (row 4) ahead of -0.0 (row 5).
            assertQuery("SELECT * FROM x ORDER BY d ASC")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            d
                            -1.0000000000000004
                            -1.0000000000000002
                            -0.0
                            0.0
                            0.5
                            1.0000000000000002
                            1.0000000000000004
                            null
                            null
                            null
                            null
                            """);
            assertQuery("SELECT * FROM x ORDER BY d DESC")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            d
                            null
                            null
                            null
                            null
                            1.0000000000000004
                            1.0000000000000002
                            0.5
                            0.0
                            -0.0
                            -1.0000000000000002
                            -1.0000000000000004
                            """);
        });
    }

    @Test
    public void testOrderByFloatEdgeCases() throws Exception {
        Assume.assumeTrue(sortMode == SortMode.SORT_ENABLED);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x AS (
                                SELECT CAST(CASE x
                                    WHEN 1 THEN CAST('NaN' AS DOUBLE)
                                    WHEN 2 THEN CAST('-Infinity' AS DOUBLE)
                                    WHEN 3 THEN CAST('Infinity' AS DOUBLE)
                                    WHEN 4 THEN 0.0
                                    WHEN 5 THEN -0.0
                                    WHEN 6 THEN 1.0001
                                    WHEN 7 THEN 1.0002
                                    WHEN 8 THEN -1.0001
                                    WHEN 9 THEN -1.0002
                                    WHEN 10 THEN CAST('NaN' AS DOUBLE)
                                    ELSE 0.5
                                END AS FLOAT) AS f
                                FROM long_sequence(11)
                            )"""
            );
            assertQuery("SELECT * FROM x ORDER BY f ASC")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            f
                            -1.0002
                            -1.0001
                            -0.0
                            0.0
                            0.5
                            1.0001
                            1.0002
                            null
                            null
                            null
                            null
                            """);
        });
    }

    @Test
    public void testOrderByIPv4ColumnAscMixedValues() throws Exception {
        assertQuery("select * from x order by a asc;")
                .ddl("create table x as (" +
                        "select" +
                        " cast (cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as int) as IPv4) as a" +
                        " from long_sequence(25)" +
                        ")")
                .expectSize()
                .returns("""
                        a
                        
                        
                        
                        
                        
                        
                        0.0.0.1
                        0.0.0.2
                        0.0.0.3
                        0.0.0.4
                        0.0.0.5
                        0.0.0.6
                        0.0.0.7
                        0.0.0.8
                        0.0.0.9
                        255.255.255.246
                        255.255.255.247
                        255.255.255.248
                        255.255.255.249
                        255.255.255.250
                        255.255.255.251
                        255.255.255.252
                        255.255.255.253
                        255.255.255.254
                        255.255.255.255
                        """);
    }

    @Test
    public void testOrderByIPv4ColumnDescMixedValues() throws Exception {
        assertQuery("select * from x order by a desc;")
                .ddl("create table x as (" +
                        "select" +
                        " cast (cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as int) as IPv4) as a" +
                        " from long_sequence(25)" +
                        ")")
                .expectSize()
                .returns("""
                        a
                        255.255.255.255
                        255.255.255.254
                        255.255.255.253
                        255.255.255.252
                        255.255.255.251
                        255.255.255.250
                        255.255.255.249
                        255.255.255.248
                        255.255.255.247
                        255.255.255.246
                        0.0.0.9
                        0.0.0.8
                        0.0.0.7
                        0.0.0.6
                        0.0.0.5
                        0.0.0.4
                        0.0.0.3
                        0.0.0.2
                        0.0.0.1
                        
                        
                        
                        
                        
                        
                        """);
    }

    @Test
    public void testOrderByIntColumnDescMixedValues() throws Exception {
        assertQuery("select * from x order by a desc;")
                .ddl("create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as int) as a" +
                        " from long_sequence(25)" +
                        ")")
                .expectSize()
                .returns("""
                        a
                        9
                        8
                        7
                        6
                        5
                        4
                        3
                        2
                        1
                        0
                        -1
                        -2
                        -3
                        -4
                        -5
                        -6
                        -7
                        -8
                        -9
                        -10
                        null
                        null
                        null
                        null
                        null
                        """);
    }

    @Test
    public void testOrderByLimitCompactionBottomK() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT x AS v FROM long_sequence(50_000))");
            assertQuery("SELECT * FROM x ORDER BY v LIMIT -3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            49998
                            49999
                            50000
                            """);
        });
    }

    @Test
    public void testOrderByLimitCompactionLargeLimitUnderTightMemoryCap() throws Exception {
        // The caps fit ~32K entries and the limit exceeds half of that, so compaction
        // must still fire at the budget instead of overflowing.
        Assume.assumeTrue(sortMode == SortMode.SORT_ENABLED);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES, 262_144);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES, 262_144);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT x AS v FROM long_sequence(50_000))");
            // A bind-variable limit keeps the plan off the constant-limit top-K factories.
            bindVariableService.clear();
            bindVariableService.setLong("n", 20_000);
            assertQuery("SELECT count(*) cnt, min(v) mn, max(v) mx FROM (SELECT * FROM x ORDER BY v LIMIT :n)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            cnt\tmn\tmx
                            20000\t1\t20000
                            """);
        });
    }

    @Test
    public void testOrderByLimitCompactionTopKAsc() throws Exception {
        assertMemoryLeak(() -> {
            // 50,000 rows exceed the compaction trigger, so the build exercises the
            // threshold-reject and compact paths; ascending input with ORDER BY v
            // takes the all-rejected path, DESC the all-accepted path. The bind-variable
            // limit keeps the plan off the constant-limit top-K factories.
            execute("CREATE TABLE x AS (SELECT x AS v FROM long_sequence(50_000))");
            bindVariableService.clear();
            bindVariableService.setLong("n", 3);
            assertQuery("SELECT * FROM x ORDER BY v LIMIT :n")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining(limitedSortPlanType())
                    .returns("""
                            v
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testOrderByLimitCompactionTopKDesc() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT x AS v FROM long_sequence(50_000))");
            bindVariableService.clear();
            bindVariableService.setLong("n", 3);
            assertQuery("SELECT * FROM x ORDER BY v DESC LIMIT :n")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            50000
                            49999
                            49998
                            """);
        });
    }

    @Test
    public void testOrderByLimitCompactionWithRange() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT x AS v FROM long_sequence(50_000))");
            assertQuery("SELECT * FROM x ORDER BY v LIMIT 2,5")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            3
                            4
                            5
                            """);
        });
    }

    @Test
    public void testOrderByLimitDegenerateShapes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT x AS v FROM long_sequence(5))");
            assertQuery("SELECT * FROM x ORDER BY v LIMIT 0")
                    .noLeakCheck()
                    .expectSize()
                    .returns("v\n");
            assertQuery("SELECT * FROM x ORDER BY v LIMIT 3,3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("v\n");
            // lo > hi normalizes to rows hi..lo
            assertQuery("SELECT * FROM x ORDER BY v LIMIT 5,3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            4
                            5
                            """);
            assertQuery("SELECT * FROM x ORDER BY v LIMIT -3,-3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("v\n");
            assertQuery("SELECT * FROM x ORDER BY v LIMIT -2,-4")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            2
                            3
                            """);
        });
    }

    @Test
    public void testOrderByLimitLoPosHiNegBindVariableFirstExecution() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT x AS v FROM long_sequence(5))");
            bindVariableService.clear();
            bindVariableService.setLong("lo", 1);
            bindVariableService.setLong("hi", -1);
            assertQuery("SELECT * FROM x ORDER BY v LIMIT :lo, :hi")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            2
                            3
                            4
                            """);
        });
    }

    @Test
    public void testOrderByLimitLoPosHiNegBindVariableReExecution() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT x AS v FROM long_sequence(5))");
            bindVariableService.clear();
            bindVariableService.setLong("lo", 1);
            bindVariableService.setLong("hi", 3);
            try (RecordCursorFactory factory = select("SELECT * FROM x ORDER BY v LIMIT :lo, :hi")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("v\n2\n3\n", cursor, factory.getMetadata());
                }

                // same factory, lo >= 0 / hi < 0: slice off one row at each end
                bindVariableService.setLong("hi", -1);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("v\n2\n3\n4\n", cursor, factory.getMetadata());
                }

                // and back to the top-K shape
                bindVariableService.setLong("hi", 3);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("v\n2\n3\n", cursor, factory.getMetadata());
                }
            }
        });
    }

    @Test
    public void testOrderByLimitLoPosHiNegBindVariableReExecutionDesignatedTimestamp() throws Exception {
        // Re-execution over a designated-timestamp table whose ORDER BY starts with the
        // timestamp, so the factory selects the legacy partially-sorted cursor. A non-encodable
        // varchar sort key keeps the encoded path out, so both flag parameterizations route here.
        // Execution 1 (lo>=0, hi>=0) picks the partially-sorted cursor; execution 2 re-binds to
        // (lo>=0, hi<0), installing the unbounded limit sentinel (-1). The early-stop must scan all
        // timestamp groups for a negative limit, not bail at the first group boundary.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT x AS v, ('k' || x)::varchar AS s, (x * 1_000_000)::timestamp AS ts " +
                            "FROM long_sequence(7)" +
                            ") TIMESTAMP(ts)"
            );
            bindVariableService.clear();
            bindVariableService.setLong("lo", 1);
            bindVariableService.setLong("hi", 3);
            try (RecordCursorFactory factory = select("SELECT * FROM x ORDER BY ts, s LIMIT :lo, :hi")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("""
                            v\ts\tts
                            2\tk2\t1970-01-01T00:00:02.000000Z
                            3\tk3\t1970-01-01T00:00:03.000000Z
                            """, cursor, factory.getMetadata());
                }

                // same factory, lo >= 0 / hi < 0: slice off one row at each end of the full set
                bindVariableService.setLong("hi", -1);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("""
                            v\ts\tts
                            2\tk2\t1970-01-01T00:00:02.000000Z
                            3\tk3\t1970-01-01T00:00:03.000000Z
                            4\tk4\t1970-01-01T00:00:04.000000Z
                            5\tk5\t1970-01-01T00:00:05.000000Z
                            6\tk6\t1970-01-01T00:00:06.000000Z
                            """, cursor, factory.getMetadata());
                }

                // and back to the top-K shape
                bindVariableService.setLong("hi", 3);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("""
                            v\ts\tts
                            2\tk2\t1970-01-01T00:00:02.000000Z
                            3\tk3\t1970-01-01T00:00:03.000000Z
                            """, cursor, factory.getMetadata());
                }
            }
        });
    }

    @Test
    public void testOrderByLimitLoPosHiNullBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT x AS v FROM long_sequence(5))");
            bindVariableService.clear();
            bindVariableService.setLong("lo", 1);
            bindVariableService.setLong("hi", Numbers.LONG_NULL);
            assertQuery("SELECT * FROM x ORDER BY v LIMIT :lo, :hi")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            """);
        });
    }

    @Test
    public void testOrderByLimitNegatedNaNBoundary() throws Exception {
        assertMemoryLeak(() -> {
            // NULL doubles are NaN and negating them flips the NaN sign bit; the
            // encoder canonicalizes NaN so the LIMIT cut selects the same rows as
            // the legacy comparator on both sides of the boundary.
            execute("CREATE TABLE nd AS (SELECT CASE WHEN x <= 3 THEN x::double END val FROM long_sequence(5))");
            bindVariableService.clear();
            bindVariableService.setLong("n", 3);
            assertQuery("SELECT val, -val neg FROM nd ORDER BY neg LIMIT :n")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            val\tneg
                            3.0\t-3.0
                            2.0\t-2.0
                            1.0\t-1.0
                            """);
            assertQuery("SELECT val, -val neg FROM nd ORDER BY neg DESC LIMIT :n")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            val\tneg
                            null\tnull
                            null\tnull
                            1.0\t-1.0
                            """);
        });
    }

    @Test
    public void testOrderByLimitNullBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT x AS v FROM long_sequence(5))");
            bindVariableService.clear();
            bindVariableService.setLong("lo", Numbers.LONG_NULL);
            assertQuery("SELECT * FROM x ORDER BY v LIMIT :lo")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            1
                            2
                            3
                            4
                            5
                            """);
        });
    }

    @Test
    public void testOrderByLimitNullBindVariableRanges() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT x AS v FROM long_sequence(5))");
            final String query = "SELECT * FROM x ORDER BY v LIMIT :lo, :hi";

            // NULL lo: keep everything up to 2 rows from the end
            bindVariableService.clear();
            bindVariableService.setLong("lo", Numbers.LONG_NULL);
            bindVariableService.setLong("hi", -2);
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            1
                            2
                            3
                            """);

            // NULL hi: keep everything up to 3 rows from the end
            bindVariableService.clear();
            bindVariableService.setLong("lo", -3);
            bindVariableService.setLong("hi", Numbers.LONG_NULL);
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            1
                            2
                            """);

            // NULL lo and hi: lo == hi produces an empty result
            bindVariableService.clear();
            bindVariableService.setLong("lo", Numbers.LONG_NULL);
            bindVariableService.setLong("hi", Numbers.LONG_NULL);
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            """);

            // NULL lo with a positive hi: rows from the start
            bindVariableService.clear();
            bindVariableService.setLong("lo", Numbers.LONG_NULL);
            bindVariableService.setLong("hi", 2);
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            1
                            2
                            """);
        });
    }

    @Test
    public void testOrderByLimitNullLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT x AS v FROM long_sequence(5))");
            assertQuery("SELECT * FROM x ORDER BY v LIMIT null::long")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan((sortMode == SortMode.SORT_ENABLED ? "Encode sort light" : "Sort light") + """
                             lo: nullL
                              keys: [v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """)
                    .returns("""
                            v
                            1
                            2
                            3
                            4
                            5
                            """);
        });
    }

    @Test
    public void testOrderByLimitParquetFilterWideProjection() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE pq AS (
                        SELECT
                            x v,
                            ('r' || x)::varchar note,
                            ARRAY[x::double, (x * 2)::double] arr,
                            ('s' || (x % 3))::symbol sym,
                            timestamp_sequence(0, 100) ts
                        FROM long_sequence(10_000)
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("INSERT INTO pq VALUES (1_000_000, 'r1000000', ARRAY[1000000.0, 2000000.0], 's0', '2000-01-01')");
            execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2000-01-01'");
            // Reordered projection routes the sort key through the SelectedRecordCursor
            // remap; the filter engages worker-side late materialization. The sort key
            // must stay out of the filter so the build reads it only through the
            // remapped parent-used-columns decode - a broken remap fails the test.
            // The two-bound limit keeps the plan off the constant-limit top-K factories.
            assertQuery("SELECT note, v, sym FROM pq WHERE sym = 's1' AND length(note) > 1 ORDER BY v DESC LIMIT 0,3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining(limitedSortPlanType(), "Async")
                    .returns("""
                            note\tv\tsym
                            r10000\t10000\ts1
                            r9997\t9997\ts1
                            r9994\t9994\ts1
                            """);
        });
    }

    @Test
    public void testOrderByLimitParquetFilterWideProjectionMultiWorker() throws Exception {
        // Same wide-projection encoded top-K over Parquet as
        // testOrderByLimitParquetFilterWideProjection, but driven through a real
        // 4-worker pool over many Parquet partitions (one page frame each). The async
        // filter reduces frames across worker threads, so the encoded sort's
        // setParentUsedColumns publication of AsyncFilterAtom.lateMatSkipColumnIndexes
        // is read cross-thread instead of inline. The selective sym filter (10%) keeps
        // late materialization engaged on every frame. Only the encoded path declares
        // the used columns; the legacy path runs the same query for a correctness check.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, _, sqlExecutionContext) -> {
                        engine.execute(
                                """
                                        CREATE TABLE pq AS (
                                            SELECT
                                                x v,
                                                ('r' || x)::varchar note,
                                                ARRAY[x::double, (x * 2)::double] arr,
                                                ('s' || (x % 10))::symbol sym,
                                                timestamp_sequence(0, 172_800_000) ts
                                            FROM long_sequence(6000)
                                        ) TIMESTAMP(ts) PARTITION BY DAY""",
                                sqlExecutionContext
                        );
                        engine.execute("INSERT INTO pq VALUES (1_000_000, 'r1000000', ARRAY[1000000.0, 2000000.0], 's0', '2000-01-01')", sqlExecutionContext);
                        engine.execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2000-01-01'", sqlExecutionContext);

                        assertQuery("SELECT note, v, arr, sym FROM pq WHERE sym = 's7' AND length(note) > 1 ORDER BY v DESC LIMIT 0,5")
                                .withEngine(engine)
                                .withContext(sqlExecutionContext)
                                .noLeakCheck()
                                .expectSize()
                                .withPlanContaining(limitedSortPlanType(), "Async")
                                .returns("""
                                        note\tv\tarr\tsym
                                        r5997\t5997\t[5997.0,11994.0]\ts7
                                        r5987\t5987\t[5987.0,11974.0]\ts7
                                        r5977\t5977\t[5977.0,11954.0]\ts7
                                        r5967\t5967\t[5967.0,11934.0]\ts7
                                        r5957\t5957\t[5957.0,11914.0]\ts7
                                        """);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testOrderByLimitParquetRowFilteredEmit() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE pq AS (
                        SELECT
                            x v,
                            ('r' || x)::varchar note,
                            ARRAY[x::double, (x * 2)::double] arr,
                            ('s' || (x % 3))::symbol sym,
                            timestamp_sequence(0, 100) ts
                        FROM long_sequence(10_000)
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("INSERT INTO pq VALUES (1_000_000, 'r1000000', ARRAY[1000000.0, 2000000.0], 's0', '2000-01-01')");
            execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2000-01-01'");

            // Two-bound limits are not top-K candidates, so the plan stays on the
            // encoded sort and its emit phase exercises row-filtered Parquet decode.
            assertQuery("SELECT v, note, arr, sym FROM pq ORDER BY v LIMIT 0,3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining(limitedSortPlanType())
                    .returns("""
                            v\tnote\tarr\tsym
                            1\tr1\t[1.0,2.0]\ts1
                            2\tr2\t[2.0,4.0]\ts2
                            3\tr3\t[3.0,6.0]\ts0
                            """);

            // Bottom-K spans the native and the Parquet partition.
            assertQuery("SELECT v, note, arr, sym FROM pq ORDER BY v DESC LIMIT 0,3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v\tnote\tarr\tsym
                            1000000\tr1000000\t[1000000.0,2000000.0]\ts0
                            10000\tr10000\t[10000.0,20000.0]\ts1
                            9999\tr9999\t[9999.0,19998.0]\ts0
                            """);

            assertQuery("SELECT v, note, arr, sym FROM pq ORDER BY v LIMIT 2,5")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v\tnote\tarr\tsym
                            3\tr3\t[3.0,6.0]\ts0
                            4\tr4\t[4.0,8.0]\ts1
                            5\tr5\t[5.0,10.0]\ts2
                            """);
        });
    }

    @Test
    public void testOrderByLimitParquetRowFilteredEmitNoLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE pqu AS (
                        SELECT x v, ('r' || x)::varchar note, timestamp_sequence(0, 100) ts
                        FROM long_sequence(50)
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("INSERT INTO pqu VALUES (1_000_000, 'r1000000', '2000-01-01')");
            execute("ALTER TABLE pqu CONVERT PARTITION TO PARQUET WHERE ts < '2000-01-01'");
            // The filter keeps 10 of the 50 parquet rows, under the 50% density gate,
            // so the unlimited sort's emit declaration row-filters the parquet decode.
            assertQuery("SELECT v, note FROM pqu WHERE v % 5 = 3 ORDER BY v DESC")
                    .noLeakCheck()
                    .withPlanContaining(limitedSortPlanType())
                    .returns("""
                            v\tnote
                            48\tr48
                            43\tr43
                            38\tr38
                            33\tr33
                            28\tr28
                            23\tr23
                            18\tr18
                            13\tr13
                            8\tr8
                            3\tr3
                            """);
        });
    }

    @Test
    public void testOrderByLimitParquetRowFilteredEmitMultiRowGroup() throws Exception {
        // The single-row-group fixtures always declare emit rows in row group 0 from
        // offset 0, so an off-by-rowGroupLo or wrong-slice-segment bug in the row-filtered
        // decode passes them silently. Force 10 row groups in one Parquet partition and
        // land the declarations past group 0 / past offset 0 to catch that.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1_000);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE pqg AS (
                        SELECT x v, ('r' || x)::varchar note, timestamp_sequence(0, 100) ts
                        FROM long_sequence(10_000)
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("INSERT INTO pqg VALUES (1_000_000, 'r1000000', '2000-01-01')");
            execute("ALTER TABLE pqg CONVERT PARTITION TO PARQUET WHERE ts < '2000-01-01'");

            // One matching row per row group keeps the emit declaration well under the
            // density gate; LIMIT 3,8 drops the first three matches so the surviving
            // declarations sit in row groups 3..7, each at offset 136 within its group.
            assertQuery("SELECT v, note FROM pqg WHERE v % 1000 = 137 ORDER BY v LIMIT 3,8")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining(limitedSortPlanType())
                    .returns("""
                            v\tnote
                            3137\tr3137
                            4137\tr4137
                            5137\tr5137
                            6137\tr6137
                            7137\tr7137
                            """);

            // An interval on the designated timestamp starts the scanned Parquet frame
            // mid-row-group (row 2500 sits at offset 500 in group 2), so the declared top
            // rows decode through a non-zero rowGroupLo.
            assertQuery("SELECT v, note FROM pqg WHERE ts >= '1970-01-01T00:00:00.250000Z' ORDER BY v LIMIT 0,3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining(limitedSortPlanType())
                    .returns("""
                            v\tnote
                            2501\tr2501
                            2502\tr2502
                            2503\tr2503
                            """);
        });
    }

    @Test
    public void testOrderByLimitSmallLimitUnderTightMemoryCap() throws Exception {
        // The caps fit ~32K entries; the 50,000-row scan overflows them without
        // compaction. The tree-chain path holds only `limit` entries, so both
        // parameterized modes must return the top rows instead of throwing.
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES, 262_144);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES, 262_144);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT x AS v FROM long_sequence(50_000))");
            bindVariableService.clear();
            bindVariableService.setLong("n", 2);
            assertQuery("SELECT * FROM x ORDER BY v LIMIT :n")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            1
                            2
                            """);
        });
    }

    @Test
    public void testOrderByLimitThresholdTieSelection() throws Exception {
        Assume.assumeTrue(sortMode == SortMode.SORT_ENABLED);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x % 10 k, x rid FROM long_sequence(50_000))");
            bindVariableService.clear();
            bindVariableService.setLong("n", 9_000);
            assertQuery("""
                    SELECT
                        sum(CASE WHEN k = 1 THEN 1 ELSE 0 END) cnt,
                        min(CASE WHEN k = 1 THEN rid END) mn,
                        max(CASE WHEN k = 1 THEN rid END) mx
                    FROM (SELECT * FROM t ORDER BY k LIMIT :n)""")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            cnt\tmn\tmx
                            4000\t1\t39991
                            """);
        });
    }

    @Test
    public void testOrderByLimitTimestampEarlyStopDesc() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE esd AS (
                        SELECT ((x - 1) / 3 * 1_000_000)::timestamp ts, 10 - x v
                        FROM long_sequence(9)
                    ) TIMESTAMP(ts)""");
            assertQuery("SELECT * FROM esd ORDER BY ts DESC, v LIMIT 4")
                    .noLeakCheck()
                    .expectSize()
                    .timestampDesc("ts")
                    .withPlanContaining(limitedSortPlanType(), "partiallySorted: true")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:02.000000Z\t1
                            1970-01-01T00:00:02.000000Z\t2
                            1970-01-01T00:00:02.000000Z\t3
                            1970-01-01T00:00:01.000000Z\t4
                            """);
        });
    }

    @Test
    public void testOrderByLimitTimestampEarlyStopGroupSpansCompaction() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE esc AS (
                        SELECT
                            (CASE WHEN x <= 5_000 THEN 0 ELSE 1_000_000 END)::timestamp ts,
                            x % 5_000 v
                        FROM long_sequence(5_005)
                    ) TIMESTAMP(ts)""");
            bindVariableService.clear();
            bindVariableService.setLong("n", 3);
            assertQuery("SELECT * FROM esc ORDER BY ts, v LIMIT :n")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000000Z\t0
                            1970-01-01T00:00:00.000000Z\t1
                            1970-01-01T00:00:00.000000Z\t2
                            """);
        });
    }

    @Test
    public void testOrderByLimitTimestampEarlyStopGroupStraddle() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE es AS (
                        SELECT
                            ((x - 1) / 3 * 1_000_000)::timestamp ts,
                            10 - x v
                        FROM long_sequence(9)
                    ) TIMESTAMP(ts)""");

            // The limit lands mid-group: the second group must be scanned in full.
            assertQuery("SELECT * FROM es ORDER BY ts, v LIMIT 4")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining(limitedSortPlanType(), "partiallySorted: true")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000000Z\t7
                            1970-01-01T00:00:00.000000Z\t8
                            1970-01-01T00:00:00.000000Z\t9
                            1970-01-01T00:00:01.000000Z\t4
                            """);

            // The limit lands one row past a completed group boundary.
            assertQuery("SELECT * FROM es ORDER BY ts, v LIMIT 7")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000000Z\t7
                            1970-01-01T00:00:00.000000Z\t8
                            1970-01-01T00:00:00.000000Z\t9
                            1970-01-01T00:00:01.000000Z\t4
                            1970-01-01T00:00:01.000000Z\t5
                            1970-01-01T00:00:01.000000Z\t6
                            1970-01-01T00:00:02.000000Z\t1
                            """);
        });
    }

    @Test
    public void testOrderByLimitTinyMemoryCap() throws Exception {
        // The caps fit fewer entries than the minimum compaction trigger, so the
        // trigger must clamp to the budget.
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES, 16_384);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES, 16_384);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT x AS v FROM long_sequence(5_000))");
            bindVariableService.clear();
            bindVariableService.setLong("n", 10);
            assertQuery("SELECT * FROM x ORDER BY v LIMIT :n")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            1
                            2
                            3
                            4
                            5
                            6
                            7
                            8
                            9
                            10
                            """);
        });
    }

    @Test
    public void testOrderByLimitUnboundedOverflowUnderTightMemoryCap() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES, 262_144);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES, 262_144);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT x AS v FROM long_sequence(50_000))");
            assertQuery("SELECT * FROM x ORDER BY v LIMIT null::long")
                    .noLeakCheck()
                    .failsWith("memory exceeded");
        });
    }

    @Test
    public void testOrderByLongColumnAscMixedValues() throws Exception {
        assertQuery("select * from x order by a asc;")
                .ddl("create table x as (" +
                        "select" +
                        " case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as a" +
                        " from long_sequence(25)" +
                        ")")
                .expectSize()
                .returns("""
                        a
                        null
                        null
                        null
                        null
                        null
                        -10
                        -9
                        -8
                        -7
                        -6
                        -5
                        -4
                        -3
                        -2
                        -1
                        0
                        1
                        2
                        3
                        4
                        5
                        6
                        7
                        8
                        9
                        """);
    }

    @Test
    public void testOrderByLongColumnDescMixedValues() throws Exception {
        assertQuery("select * from x order by a desc;")
                .ddl("create table x as (" +
                        "select" +
                        " case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as a" +
                        " from long_sequence(25)" +
                        ")")
                .expectSize()
                .returns("""
                        a
                        9
                        8
                        7
                        6
                        5
                        4
                        3
                        2
                        1
                        0
                        -1
                        -2
                        -3
                        -4
                        -5
                        -6
                        -7
                        -8
                        -9
                        -10
                        null
                        null
                        null
                        null
                        null
                        """);
    }

    @Test
    public void testOrderByMultiColumnIntAndLongAsc() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x AS (
                                SELECT
                                    CAST(CASE
                                        WHEN x % 3 = 0 THEN 1
                                        WHEN x % 3 = 1 THEN 2
                                        ELSE 3
                                    END AS INT) AS a,
                                    CASE
                                        WHEN x <= 3 THEN 30 - x
                                        WHEN x <= 6 THEN 60 - x
                                        ELSE 90 - x
                                    END AS b
                                FROM long_sequence(9)
                            )"""
            );
            assertQuery("SELECT * FROM x ORDER BY a, b")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a	b
                            1	27
                            1	54
                            1	81
                            2	29
                            2	56
                            2	83
                            3	28
                            3	55
                            3	82
                            """);
        });
    }

    @Test
    public void testOrderByMultiColumnMixedDirectionsWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x AS (
                                SELECT
                                    CAST(CASE
                                        WHEN x <= 2 THEN NULL
                                        WHEN x <= 5 THEN 1
                                        WHEN x <= 8 THEN 2
                                        ELSE 3
                                    END AS INT) AS a,
                                    CAST(CASE
                                        WHEN x % 3 = 1 THEN 100
                                        WHEN x % 3 = 2 THEN 200
                                        ELSE 300
                                    END AS LONG) AS b
                                FROM long_sequence(11)
                            )"""
            );
            assertQuery("SELECT * FROM x ORDER BY a DESC, b ASC")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a	b
                            3	100
                            3	200
                            3	300
                            2	100
                            2	200
                            2	300
                            1	100
                            1	200
                            1	300
                            null	100
                            null	200
                            """);
        });
    }

    @Test
    public void testOrderByMultiColumnSymbolAndLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL, val LONG)");
            execute(
                    """
                            INSERT INTO x (sym, val) VALUES
                            ('B', 2), ('A', 3), ('B', 1), ('A', 1), ('B', 3), ('A', 2)"""
            );
            assertQuery("SELECT * FROM x ORDER BY sym, val")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            sym\tval
                            A\t1
                            A\t2
                            A\t3
                            B\t1
                            B\t2
                            B\t3
                            """);
        });
    }

    @Test
    public void testOrderByOneRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT 42 AS a FROM long_sequence(1))");
            assertQuery("SELECT * FROM x ORDER BY a")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a
                            42
                            """);
        });
    }

    @Test
    public void testOrderByStableSort() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x AS (
                                SELECT
                                    1 AS key,
                                    x AS insertion_order
                                FROM long_sequence(20)
                            )"""
            );
            assertQuery("SELECT * FROM x ORDER BY key")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            key\tinsertion_order
                            1\t1
                            1\t2
                            1\t3
                            1\t4
                            1\t5
                            1\t6
                            1\t7
                            1\t8
                            1\t9
                            1\t10
                            1\t11
                            1\t12
                            1\t13
                            1\t14
                            1\t15
                            1\t16
                            1\t17
                            1\t18
                            1\t19
                            1\t20
                            """);
        });
    }

    @Test
    public void testOrderBySymbolColumnAscWithNulls() throws Exception {
        assertQuery("SELECT * FROM x ORDER BY sym ASC")
                .ddl("CREATE TABLE x AS (" +
                        "SELECT" +
                        " CAST(CASE" +
                        "     WHEN x % 4 = 0 THEN NULL" +
                        "     WHEN x % 4 = 1 THEN 'A'" +
                        "     WHEN x % 4 = 2 THEN 'B'" +
                        "     ELSE 'C'" +
                        " END AS SYMBOL) AS sym," +
                        " x AS val" +
                        " FROM long_sequence(12)" +
                        ")")
                .expectSize()
                .returns("""
                        sym\tval
                        \t4
                        \t8
                        \t12
                        A\t1
                        A\t5
                        A\t9
                        B\t2
                        B\t6
                        B\t10
                        C\t3
                        C\t7
                        C\t11
                        """);
    }

    @Test
    public void testOrderBySymbolColumnDescWithNulls() throws Exception {
        assertQuery("SELECT * FROM x ORDER BY sym DESC")
                .ddl("CREATE TABLE x AS (" +
                        "SELECT" +
                        " CAST(CASE" +
                        "     WHEN x % 4 = 0 THEN NULL" +
                        "     WHEN x % 4 = 1 THEN 'A'" +
                        "     WHEN x % 4 = 2 THEN 'B'" +
                        "     ELSE 'C'" +
                        " END AS SYMBOL) AS sym," +
                        " x AS val" +
                        " FROM long_sequence(12)" +
                        ")")
                .expectSize()
                .returns("""
                        sym\tval
                        C\t3
                        C\t7
                        C\t11
                        B\t2
                        B\t6
                        B\t10
                        A\t1
                        A\t5
                        A\t9
                        \t4
                        \t8
                        \t12
                        """);
    }

    @Test
    public void testOrderBySymbolColumnLimitWithNulls() throws Exception {
        // The fuzz test appends a unique key to the ORDER BY so the sort is total and
        // the LIMIT cut never splits a tie group: the encoded top-K and the legacy
        // tree-chain resolve ties differently, so a symbol-only ORDER BY with the cut
        // inside a tie diverges between them. This pins the case the fuzzer skips - the
        // encoded top-K's own NULL-symbol ordering at the LIMIT cut - so it asserts the
        // encoded path only. The encoded sort appends the rowId as the trailing key
        // word, so ties break rowId-ascending (here val-ascending) deterministically.
        Assume.assumeTrue(sortMode == SortMode.SORT_ENABLED);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "SELECT" +
                    " CAST(CASE" +
                    "     WHEN x % 4 = 0 THEN NULL" +
                    "     WHEN x % 4 = 1 THEN 'A'" +
                    "     WHEN x % 4 = 2 THEN 'B'" +
                    "     ELSE 'C'" +
                    " END AS SYMBOL) AS sym," +
                    " x AS val" +
                    " FROM long_sequence(12)" +
                    ")");

            // ASC: NULL sorts first, so a cut inside the NULL group returns the
            // rowId-smallest NULL rows.
            assertQuery("SELECT * FROM x ORDER BY sym ASC LIMIT 0,2")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining(limitedSortPlanType())
                    .returns("""
                            sym\tval
                            \t4
                            \t8
                            """);

            // ASC: cut exactly at the NULL/'A' boundary returns the whole NULL group.
            assertQuery("SELECT * FROM x ORDER BY sym ASC LIMIT 0,3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining(limitedSortPlanType())
                    .returns("""
                            sym\tval
                            \t4
                            \t8
                            \t12
                            """);

            // ASC: cut one past the NULL group keeps every NULL plus the first 'A'.
            assertQuery("SELECT * FROM x ORDER BY sym ASC LIMIT 0,4")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining(limitedSortPlanType())
                    .returns("""
                            sym\tval
                            \t4
                            \t8
                            \t12
                            A\t1
                            """);

            // DESC: NULL sorts last, so a small cut must exclude every NULL row.
            assertQuery("SELECT * FROM x ORDER BY sym DESC LIMIT 0,3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining(limitedSortPlanType())
                    .returns("""
                            sym\tval
                            C\t3
                            C\t7
                            C\t11
                            """);

            // DESC: cut exactly at the 'A'/NULL boundary keeps everything but the NULLs.
            assertQuery("SELECT * FROM x ORDER BY sym DESC LIMIT 0,9")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining(limitedSortPlanType())
                    .returns("""
                            sym\tval
                            C\t3
                            C\t7
                            C\t11
                            B\t2
                            B\t6
                            B\t10
                            A\t1
                            A\t5
                            A\t9
                            """);

            // DESC: cut one past the 'A' group reaches the first (rowId-smallest) NULL.
            assertQuery("SELECT * FROM x ORDER BY sym DESC LIMIT 0,10")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining(limitedSortPlanType())
                    .returns("""
                            sym\tval
                            C\t3
                            C\t7
                            C\t11
                            B\t2
                            B\t6
                            B\t10
                            A\t1
                            A\t5
                            A\t9
                            \t4
                            """);
        });
    }

    @Test
    public void testOrderByTimestampColumnAscMixedValues() throws Exception {
        assertQuery("select * from x order by a asc;")
                .ddl("create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as timestamp) as a" +
                        " from long_sequence(25)" +
                        ")")
                .timestamp("a")
                .expectSize()
                .returns("""
                        a
                        
                        
                        
                        
                        
                        1969-12-31T23:59:59.999990Z
                        1969-12-31T23:59:59.999991Z
                        1969-12-31T23:59:59.999992Z
                        1969-12-31T23:59:59.999993Z
                        1969-12-31T23:59:59.999994Z
                        1969-12-31T23:59:59.999995Z
                        1969-12-31T23:59:59.999996Z
                        1969-12-31T23:59:59.999997Z
                        1969-12-31T23:59:59.999998Z
                        1969-12-31T23:59:59.999999Z
                        1970-01-01T00:00:00.000000Z
                        1970-01-01T00:00:00.000001Z
                        1970-01-01T00:00:00.000002Z
                        1970-01-01T00:00:00.000003Z
                        1970-01-01T00:00:00.000004Z
                        1970-01-01T00:00:00.000005Z
                        1970-01-01T00:00:00.000006Z
                        1970-01-01T00:00:00.000007Z
                        1970-01-01T00:00:00.000008Z
                        1970-01-01T00:00:00.000009Z
                        """);
    }

    @Test
    public void testOrderByTimestampColumnDescMixedValues() throws Exception {
        assertQuery("select * from x order by a desc;")
                .ddl("create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as timestamp) as a" +
                        " from long_sequence(25)" +
                        ")")
                .timestampDesc("a")
                .expectSize()
                .returns("""
                        a
                        1970-01-01T00:00:00.000009Z
                        1970-01-01T00:00:00.000008Z
                        1970-01-01T00:00:00.000007Z
                        1970-01-01T00:00:00.000006Z
                        1970-01-01T00:00:00.000005Z
                        1970-01-01T00:00:00.000004Z
                        1970-01-01T00:00:00.000003Z
                        1970-01-01T00:00:00.000002Z
                        1970-01-01T00:00:00.000001Z
                        1970-01-01T00:00:00.000000Z
                        1969-12-31T23:59:59.999999Z
                        1969-12-31T23:59:59.999998Z
                        1969-12-31T23:59:59.999997Z
                        1969-12-31T23:59:59.999996Z
                        1969-12-31T23:59:59.999995Z
                        1969-12-31T23:59:59.999994Z
                        1969-12-31T23:59:59.999993Z
                        1969-12-31T23:59:59.999992Z
                        1969-12-31T23:59:59.999991Z
                        1969-12-31T23:59:59.999990Z
                        
                        
                        
                        
                        
                        """);
    }

    @Test
    public void testOrderByZeroRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a INT, b LONG)");
            assertQuery("SELECT * FROM x ORDER BY a")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            """);
        });
    }

    @Test
    public void testOrderIntColumnAscMixedValues() throws Exception {
        assertQuery("select * from x order by a asc;")
                .ddl("create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as int) as a" +
                        " from long_sequence(25)" +
                        ")")
                .expectSize()
                .returns("""
                        a
                        null
                        null
                        null
                        null
                        null
                        -10
                        -9
                        -8
                        -7
                        -6
                        -5
                        -4
                        -3
                        -2
                        -1
                        0
                        1
                        2
                        3
                        4
                        5
                        6
                        7
                        8
                        9
                        """);
    }

    @Test
    public void testOrderByCastSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, a STRING)");
            execute("INSERT INTO x VALUES (1, 'c'), (2, 'a'), (3, NULL), (4, 'b'), (5, 'a')");
            assertQuery("SELECT id, a::symbol s FROM x ORDER BY s")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ts
                            3\t
                            2\ta
                            5\ta
                            4\tb
                            1\tc
                            """);
        });
    }

    @Test
    public void testOrderByFixedKeyOver32Bytes() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x AS (
                                SELECT
                                    x % 2 AS a,
                                    x % 3 AS b,
                                    x * 0 AS c,
                                    x * 0 AS d,
                                    x AS e
                                FROM long_sequence(10)
                            )"""
            );
            assertQuery("SELECT * FROM x ORDER BY a, b, c, d, e DESC")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tc\td\te
                            0\t0\t0\t0\t6
                            0\t1\t0\t0\t10
                            0\t1\t0\t0\t4
                            0\t2\t0\t0\t8
                            0\t2\t0\t0\t2
                            1\t0\t0\t0\t9
                            1\t0\t0\t0\t3
                            1\t1\t0\t0\t7
                            1\t1\t0\t0\t1
                            1\t2\t0\t0\t5
                            """);
        });
    }

    @Test
    public void testOrderByLong256ColumnAscWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, l LONG256)");
            // ids 3 and 4 straddle the all-Long.MIN_VALUE null bit pattern
            execute(
                    """
                            INSERT INTO x VALUES
                            (1, NULL),
                            (2, 0x0000000000000000000000000000000000000000000000000000000000000001),
                            (3, 0x8000000000000000800000000000000080000000000000007fffffffffffffff),
                            (4, 0x8000000000000000800000000000000080000000000000008000000000000001),
                            (5, 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff)"""
            );
            assertQuery("SELECT id FROM x ORDER BY l")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id
                            1
                            2
                            3
                            4
                            5
                            """);
        });
    }

    @Test
    public void testOrderByLong256ColumnDescWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, l LONG256)");
            execute(
                    """
                            INSERT INTO x VALUES
                            (1, NULL),
                            (2, 0x0000000000000000000000000000000000000000000000000000000000000001),
                            (3, 0x8000000000000000800000000000000080000000000000007fffffffffffffff),
                            (4, 0x8000000000000000800000000000000080000000000000008000000000000001),
                            (5, 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff)"""
            );
            assertQuery("SELECT id FROM x ORDER BY l DESC")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id
                            5
                            4
                            3
                            2
                            1
                            """);
        });
    }

    @Test
    public void testOrderByStringColumnAscWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, s STRING)");
            execute(
                    """
                            INSERT INTO x VALUES
                            (1, NULL),
                            (2, ''),
                            (3, 'a'),
                            (4, 'aaaaaaaaaaaaaaaaaaaax'),
                            (5, 'aaaaaaaaaaaaaaaaaaaay'),
                            (6, 'ab'),
                            (7, 'b'),
                            (8, 'é'),
                            (9, '中')"""
            );
            assertQuery("SELECT id FROM x ORDER BY s")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id
                            1
                            2
                            3
                            4
                            5
                            6
                            7
                            8
                            9
                            """);
        });
    }

    @Test
    public void testOrderByStringColumnDescWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, s STRING)");
            execute(
                    """
                            INSERT INTO x VALUES
                            (1, NULL),
                            (2, ''),
                            (3, 'a'),
                            (4, 'aaaaaaaaaaaaaaaaaaaax'),
                            (5, 'aaaaaaaaaaaaaaaaaaaay'),
                            (6, 'ab'),
                            (7, 'b'),
                            (8, 'é'),
                            (9, '中')"""
            );
            assertQuery("SELECT id FROM x ORDER BY s DESC")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id
                            9
                            8
                            7
                            6
                            5
                            4
                            3
                            2
                            1
                            """);
        });
    }

    @Test
    public void testOrderByStringVsVarcharSupplementaryChars() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, s STRING, v VARCHAR)");
            execute("INSERT INTO x VALUES (1, '😀', '😀'), (2, '￿', '￿')");
            // STRING compares UTF-16 code units: the U+1F600 surrogate pair (0xD83D...)
            // sorts below U+FFFF
            assertQuery("SELECT id FROM x ORDER BY s")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id
                            1
                            2
                            """);
            // VARCHAR compares UTF-8 bytes: U+FFFF (0xEF...) sorts below U+1F600 (0xF0...)
            assertQuery("SELECT id FROM x ORDER BY v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id
                            2
                            1
                            """);
        });
    }

    @Test
    public void testOrderBySymbolAndVarcharColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, v VARCHAR)");
            execute(
                    """
                            INSERT INTO x VALUES
                            ('b', 'q'),
                            ('a', 'x'),
                            (NULL, 'r'),
                            ('a', 'p'),
                            ('b', NULL)"""
            );
            assertQuery("SELECT * FROM x ORDER BY s, v DESC")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tv
                            \tr
                            a\tx
                            a\tp
                            b\tq
                            b\t
                            """);
        });
    }

    @Test
    public void testOrderByTimestampAndVarcharColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v VARCHAR)");
            execute(
                    """
                            INSERT INTO x VALUES
                            (2, 'a'),
                            (1, 'b'),
                            (2, NULL),
                            (1, 'a'),
                            (2, 'b'),
                            (1, NULL)"""
            );
            assertQuery("SELECT * FROM x ORDER BY ts, v DESC")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000001Z\tb
                            1970-01-01T00:00:00.000001Z\ta
                            1970-01-01T00:00:00.000001Z\t
                            1970-01-01T00:00:00.000002Z\tb
                            1970-01-01T00:00:00.000002Z\ta
                            1970-01-01T00:00:00.000002Z\t
                            """);
        });
    }

    @Test
    public void testOrderByUuidColumnAscWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, u UUID)");
            // ids 4 and 5 straddle the hi=lo=Long.MIN_VALUE null bit pattern
            execute(
                    """
                            INSERT INTO x VALUES
                            (1, NULL),
                            (2, '00000000-0000-0000-0000-000000000000'),
                            (3, '11111111-2222-3333-4444-555555555555'),
                            (4, '7fffffff-ffff-ffff-ffff-ffffffffffff'),
                            (5, '80000000-0000-0000-8000-000000000001'),
                            (6, 'ffffffff-ffff-ffff-ffff-ffffffffffff')"""
            );
            assertQuery("SELECT id FROM x ORDER BY u")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id
                            1
                            2
                            3
                            4
                            5
                            6
                            """);
        });
    }

    @Test
    public void testOrderByUuidColumnDescWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, u UUID)");
            execute(
                    """
                            INSERT INTO x VALUES
                            (1, NULL),
                            (2, '00000000-0000-0000-0000-000000000000'),
                            (3, '11111111-2222-3333-4444-555555555555'),
                            (4, '7fffffff-ffff-ffff-ffff-ffffffffffff'),
                            (5, '80000000-0000-0000-8000-000000000001'),
                            (6, 'ffffffff-ffff-ffff-ffff-ffffffffffff')"""
            );
            assertQuery("SELECT id FROM x ORDER BY u DESC")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id
                            6
                            5
                            4
                            3
                            2
                            1
                            """);
        });
    }

    @Test
    public void testOrderByVarcharAndLongColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v VARCHAR, l LONG)");
            execute(
                    """
                            INSERT INTO x VALUES
                            ('b', 1),
                            ('a', 2),
                            (NULL, 3),
                            ('a', 1),
                            ('b', 2),
                            (NULL, 1)"""
            );
            assertQuery("SELECT * FROM x ORDER BY v, l DESC")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v\tl
                            \t3
                            \t1
                            a\t2
                            a\t1
                            b\t2
                            b\t1
                            """);
        });
    }

    @Test
    public void testOrderByVarcharColumnAscWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, v VARCHAR)");
            execute(
                    """
                            INSERT INTO x VALUES
                            (1, NULL),
                            (2, ''),
                            (3, 'a'),
                            (4, 'aaaaaaaaaaaaaaaaaaaax'),
                            (5, 'aaaaaaaaaaaaaaaaaaaay'),
                            (6, 'ab'),
                            (7, 'b'),
                            (8, 'é'),
                            (9, '中')"""
            );
            assertQuery("SELECT id FROM x ORDER BY v")
                    .noLeakCheck()
                    .withPlanContaining(sortMode == SortMode.SORT_ENABLED ? "Encode sort light" : "Sort light")
                    .expectSize()
                    .returns("""
                            id
                            1
                            2
                            3
                            4
                            5
                            6
                            7
                            8
                            9
                            """);
        });
    }

    @Test
    public void testOrderByVarcharColumnDescWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, v VARCHAR)");
            execute(
                    """
                            INSERT INTO x VALUES
                            (1, NULL),
                            (2, ''),
                            (3, 'a'),
                            (4, 'aaaaaaaaaaaaaaaaaaaax'),
                            (5, 'aaaaaaaaaaaaaaaaaaaay'),
                            (6, 'ab'),
                            (7, 'b'),
                            (8, 'é'),
                            (9, '中')"""
            );
            assertQuery("SELECT id FROM x ORDER BY v DESC")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id
                            9
                            8
                            7
                            6
                            5
                            4
                            3
                            2
                            1
                            """);
        });
    }

    @Test
    public void testOrderByVarcharInvalidUtf8() throws Exception {
        // A VARCHAR can hold non-UTF-8 bytes (e.g. via ILP or CSV ingestion). The encoded
        // sort rejects a 0xFF byte rather than emit a wrong order; the tree path
        // (cairo.sql.order.by.sort.enabled=false) still sorts such data by unsigned bytes.
        Assume.assumeTrue(sortMode == SortMode.SORT_ENABLED);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v VARCHAR)");
            try (TableWriter writer = getWriter("x")) {
                Utf8StringSink sink = new Utf8StringSink();
                sink.putAny((byte) 0xFF);
                TableWriter.Row row = writer.newRow();
                row.putVarchar(0, sink);
                row.append();
                writer.commit();
            }
            assertQuery("SELECT * FROM x ORDER BY v")
                    .noLeakCheck()
                    .failsWith("invalid UTF-8");
        });
    }

    @Test
    public void testOrderByVarcharKeyHeapBudget() throws Exception {
        Assume.assumeTrue(sortMode == SortMode.SORT_ENABLED);
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES, 4096);
            node1.setProperty(PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES, 4096);
            // few entries, large strings: the key heap, not the entry array, busts the budget
            execute("CREATE TABLE x AS (SELECT rnd_varchar(1000, 1000, 0) v FROM long_sequence(100))");
            assertQuery("SELECT * FROM x ORDER BY v")
                    .noLeakCheck()
                    .failsWith("memory exceeded in EncodedSort");
        });
    }

    @Test
    public void testOrderByVarcharPrefixBoundaryLengths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, v VARCHAR)");
            // encoded segments of 15-18 bytes cross the 16-byte inline prefix
            execute(
                    """
                            INSERT INTO x VALUES
                            (1, 'aaaaaaaaaaaaa'),
                            (2, 'aaaaaaaaaaaaaa'),
                            (3, 'aaaaaaaaaaaaab'),
                            (4, 'aaaaaaaaaaaaaaa'),
                            (5, 'aaaaaaaaaaaaaaaa')"""
            );
            assertQuery("SELECT id FROM x ORDER BY v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id
                            1
                            2
                            4
                            5
                            3
                            """);
        });
    }

    @Test
    public void testOrderByVarcharUnionFullSort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, v VARCHAR)");
            execute("CREATE TABLE y (id INT, v VARCHAR)");
            execute("INSERT INTO x VALUES (1, 'c'), (2, 'a'), (3, NULL)");
            execute("INSERT INTO y VALUES (4, 'b'), (5, 'a'), (6, '')");
            assertQuery("SELECT * FROM (SELECT * FROM x UNION ALL SELECT * FROM y) ORDER BY v, id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tv
                            3\t
                            6\t
                            2\ta
                            5\ta
                            4\tb
                            1\tc
                            """);
        });
    }

    private String limitedSortPlanType() {
        return sortMode == SortMode.SORT_ENABLED ? "Encode sort light" : "Sort light";
    }

    public enum SortMode {
        SORT_ENABLED, DISABLED
    }
}
