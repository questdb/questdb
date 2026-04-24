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
import io.questdb.test.AbstractCairoTest;
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
            assertQueryNoLeakCheck(
                    """
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
                            """,
                    "SELECT * FROM x ORDER BY a ASC, b DESC, c, d"
            );
        });
    }

    @Test
    public void testOrderByDateColumnAscMixedValues() throws Exception {
        assertQuery(
                """
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
                        """,
                "select * from x order by a asc;",
                "create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as date) as a" +
                        " from long_sequence(25)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByDateColumnDescMixedValues() throws Exception {
        assertQuery(
                """
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
                        
                        
                        
                        
                        
                        """,
                "select * from x order by a desc;",
                "create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as date) as a" +
                        " from long_sequence(25)" +
                        ")",
                null,
                true,
                true
        );
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
            // -Infinity and Infinity are now preserved as distinct values.
            // Sort order: -Infinity < finite < Infinity < NaN.
            // All non-finite values display as "null" in nullable columns.
            assertQueryNoLeakCheck(
                    """
                            d
                            null
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
                            """,
                    "SELECT * FROM x ORDER BY d ASC"
            );
            assertQueryNoLeakCheck(
                    """
                            d
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
                            null
                            """,
                    "SELECT * FROM x ORDER BY d DESC"
            );
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
            assertQueryNoLeakCheck(
                    """
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
                            """,
                    "SELECT * FROM x ORDER BY f ASC"
            );
        });
    }

    @Test
    public void testOrderByIPv4ColumnAscMixedValues() throws Exception {
        assertQuery(
                """
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
                        """,
                "select * from x order by a asc;",
                "create table x as (" +
                        "select" +
                        " cast (cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as int) as IPv4) as a" +
                        " from long_sequence(25)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByIPv4ColumnDescMixedValues() throws Exception {
        assertQuery(
                """
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
                        
                        
                        
                        
                        
                        
                        """,
                "select * from x order by a desc;",
                "create table x as (" +
                        "select" +
                        " cast (cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as int) as IPv4) as a" +
                        " from long_sequence(25)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByIntColumnDescMixedValues() throws Exception {
        assertQuery(
                """
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
                        """,
                "select * from x order by a desc;",
                "create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as int) as a" +
                        " from long_sequence(25)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByLongColumnAscMixedValues() throws Exception {
        assertQuery(
                """
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
                        """,
                "select * from x order by a asc;",
                "create table x as (" +
                        "select" +
                        " case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as a" +
                        " from long_sequence(25)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByLongColumnDescMixedValues() throws Exception {
        assertQuery(
                """
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
                        """,
                "select * from x order by a desc;",
                "create table x as (" +
                        "select" +
                        " case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as a" +
                        " from long_sequence(25)" +
                        ")",
                null,
                true,
                true
        );
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
            assertQueryNoLeakCheck(
                    """
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
                            """,
                    "SELECT * FROM x ORDER BY a, b"
            );
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
            assertQueryNoLeakCheck(
                    """
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
                            """,
                    "SELECT * FROM x ORDER BY a DESC, b ASC"
            );
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
            assertQueryNoLeakCheck(
                    """
                            sym\tval
                            A\t1
                            A\t2
                            A\t3
                            B\t1
                            B\t2
                            B\t3
                            """,
                    "SELECT * FROM x ORDER BY sym, val"
            );
        });
    }

    @Test
    public void testOrderByOneRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT 42 AS a FROM long_sequence(1))");
            assertQueryNoLeakCheck(
                    """
                            a
                            42
                            """,
                    "SELECT * FROM x ORDER BY a"
            );
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
            assertQueryNoLeakCheck(
                    """
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
                            """,
                    "SELECT * FROM x ORDER BY key"
            );
        });
    }

    @Test
    public void testOrderBySymbolColumnAscWithNulls() throws Exception {
        assertQuery(
                """
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
                        """,
                "SELECT * FROM x ORDER BY sym ASC",
                "CREATE TABLE x AS (" +
                        "SELECT" +
                        " CAST(CASE" +
                        "     WHEN x % 4 = 0 THEN NULL" +
                        "     WHEN x % 4 = 1 THEN 'A'" +
                        "     WHEN x % 4 = 2 THEN 'B'" +
                        "     ELSE 'C'" +
                        " END AS SYMBOL) AS sym," +
                        " x AS val" +
                        " FROM long_sequence(12)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderBySymbolColumnDescWithNulls() throws Exception {
        assertQuery(
                """
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
                        """,
                "SELECT * FROM x ORDER BY sym DESC",
                "CREATE TABLE x AS (" +
                        "SELECT" +
                        " CAST(CASE" +
                        "     WHEN x % 4 = 0 THEN NULL" +
                        "     WHEN x % 4 = 1 THEN 'A'" +
                        "     WHEN x % 4 = 2 THEN 'B'" +
                        "     ELSE 'C'" +
                        " END AS SYMBOL) AS sym," +
                        " x AS val" +
                        " FROM long_sequence(12)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByTimestampColumnAscMixedValues() throws Exception {
        assertQuery(
                """
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
                        """,
                "select * from x order by a asc;",
                "create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as timestamp) as a" +
                        " from long_sequence(25)" +
                        ")",
                "a",
                true,
                true
        );
    }

    @Test
    public void testOrderByTimestampColumnDescMixedValues() throws Exception {
        assertQuery(
                """
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
                        
                        
                        
                        
                        
                        """,
                "select * from x order by a desc;",
                "create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as timestamp) as a" +
                        " from long_sequence(25)" +
                        ")",
                "a###desc",
                true,
                true
        );
    }

    @Test
    public void testOrderByZeroRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a INT, b LONG)");
            assertQueryNoLeakCheck(
                    """
                            a\tb
                            """,
                    "SELECT * FROM x ORDER BY a"
            );
        });
    }

    @Test
    public void testOrderIntColumnAscMixedValues() throws Exception {
        assertQuery(
                """
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
                        """,
                "select * from x order by a asc;",
                "create table x as (" +
                        "select" +
                        " cast (case" +
                        "     when x < 10 then x" +
                        "     when x >= 10 and x < 15 then null" +
                        "     else x - 25" +
                        " end as int) as a" +
                        " from long_sequence(25)" +
                        ")",
                null,
                true,
                true
        );
    }

    public enum SortMode {
        SORT_ENABLED, DISABLED
    }
}
