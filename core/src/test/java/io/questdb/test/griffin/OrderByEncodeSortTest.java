/*******************************************************************************
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
